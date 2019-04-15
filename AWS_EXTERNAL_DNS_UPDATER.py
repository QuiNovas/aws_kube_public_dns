#AWS_EXTERNAL_DNS_UPDATER.py

from kubernetes import client, config, watch
from pprint import pprint as pp
import boto3
import os
import dns.resolver
import json
import logging
import re
from threading import RLock, Thread
import time
import datetime
import math

logger = logging.getLogger("__main__")
logger.setLevel(logging.DEBUG)
logging.basicConfig(format=str(datetime.datetime.now()) + ' - %(message)s')
global podsInProcess
podsInProcess = []
global lock
lock = RLock()

class Updater():
    kube = ""
    aws = ""
    domain = ""
    serviceName = ""
    namespace = ""
    zoneId = ""
    zoneName = ""
    ttl = ""
    node_selector = ""
    label_selector = ""
    iterationDelay = ""
    maxPodInfoRetries = 0


    def __init__(self):
        # Initialize Kube client
        config.load_incluster_config()
        self.kube = client.CoreV1Api()
        # Route53
        self.aws = boto3.client('route53')
        # We will need these in a lot of places
        self.domain = os.environ['ZONE_NAME']
        self.serviceName = os.environ['SERVICE_ADDRESS']
        self.namespace = os.environ['NAMESPACE']
        self.zoneId = os.environ['ZONE_ID']
        self.zoneName = os.environ['ZONE_NAME']
        self.ttl = int(os.environ['TTL'])
        self.app = os.environ['APP_NAME'] # REMOVE ME
        self.node_selector = os.environ['NODE_SELECTOR']
        self.label_selector = "app=" + self.app
        if "MONITOR_DELAY" in os.environ:
            self.iterationDelay = int(os.environ["MONITOR_DELAY"])
        else:
            self.iterationDelay = 10
        if "MAX_PODINFO_RETRIES" in os.environ:
            self.maxPodInfoRetries = int(os.environ["MAX_PODINFO_RETRIES"])
        else:
            self.maxPodInfoRetries = 5

    """
    Check if the public ip of a node matches the ip arg
    """
    def getNodeIP(self, node):
      nodes = self.kube.list_node(label_selector=self.node_selector)
      for n in nodes.items:
        name = n.metadata.name
        if name == node:
          # Get the external node address
          for addr in n.status.addresses:
            if addr.type == 'ExternalIP':
              return addr.address

    def getPodsByLabel(self):
        res = self.kube.list_namespaced_pod(namespace=self.namespace, label_selector=self.label_selector)
        return res.items

    """
    Iterates a list of pods and updates their DNS records to match their current state
    """
    def updatePodDNS(self):
        for pod in self.getPodsByLabel():
            if not self.lockIt(pod.metadata.name):
                continue
            fqdn = pod.metadata.name + '.' + self.zoneName
            ip = self.lookupDNS(fqdn)
            node = str(pod.spec.node_name)
            nodeIP = self.getNodeIP(node)
            # Check and update the host A record if necessary
            if nodeIP == ip:
                logger.debug("Node IP " + nodeIP + " matches pod IP for " + pod.metadata.name)
            else:
                # Update the A record for the pod then update the service record
                logger.debug("Node IP does not match external DNS record for pod: Pod IP = " + ip + " Node IP = " + nodeIP)
                try:
                    logger.debug("Trying to update pod DNS")
                    hostDNS = self.updateHostRecord(nodeIP, pod.metadata.name, 'UPSERT')
                except Exception as e:
                    logger.debug("Updating pod " + pod.metadata.name + " Host DNS failed")
                    logger.debug(e)
            # Check the service record and update if necessary
            records = self.aws.list_resource_record_sets(HostedZoneId=self.zoneId, StartRecordName=self.serviceName, StartRecordType='A')
            for record in records["ResourceRecordSets"]:
                if record["Name"].rstrip('.') == self.serviceName and "SetIdentifier" in record and record["SetIdentifier"] == pod.metadata.name:
                    if record["ResourceRecords"][0]["Value"] != nodeIP:
                        try:
                            serviceDNS = self.updateServiceRecord(nodeIP, pod.metadata.name, 'UPSERT')
                        except Exception as e:
                            logger.debug("Updating pod " + pod.metadata.name + " Service DNS failed.")
                            logger.debug(e)

            self.unlockIt(pod.metadata.name)

    """
    Gets pod's public address from its node and returns the pod name and IP
    """
    def getPodInfo(self, pod):
        podHostname = pod.metadata.name
        try:
            nodeName = pod.spec.node_name
        except Exception as e:
            logger.debug(pod.spec)
            logger.debug(e)
            return False

        try:
            node = self.kube.read_node_status(nodeName)
        except Exception as e:
            logger.debug(e)
            return False
        for i in node.status.addresses:
            if i.type == 'ExternalIP':
                return {"address": i.address, "host": podHostname, "nodeName": nodeName}

    """
    Tests DNS records so we can return from the caller
    without doing any unneccessary work
    """
    def lookupDNS(self, host):
        r = dns.resolver.Resolver(configure=False)
        r.nameservers = ["8.8.8.8", "8.8.4.4"]
        try:
            test = r.query(host)
            return str(test[0])
        except Exception as e:
            return False

    def getHostInfo(self, pod, action):
        podHostname = pod['object']['metadata']['name']
        if 'nodeName' not in pod['object']['spec'] and action == 'd':
            return "finalize"
        elif 'nodeName' not in pod['object']['spec'] and action == 'r':
            return False
        else:
            nodeName = pod['object']['spec']['nodeName']

        try:
            node = self.kube.read_node_status(nodeName)
        except Exception as e:
            logger.debug(e)
            return False
        for i in node.status.addresses:
            if i.type == 'ExternalIP':
                if action != 'd':
                    test = self.lookupDNS(podHostname + '.' + self.domain)
                    if test == i.address:
                        logger.debug('[' + str(action) + '] DNS for host ' + podHostname + "." + self.domain + " already set to "  + i.address)
                        return False
                return {"address": i.address, "host": podHostname}

    """
    Updates, creates, or deletes a record for a single pod
    Add a label to the pod that gives the public IP address
    Updates are done with UPSERT, deletes with DELETE action in data sent to AWS
    """
    def updateHostRecord(self, addr, host, action):
        fqdn = host + '.' + self.zoneName
        batch = {
            'Changes': [{
            'Action': action,
            'ResourceRecordSet': {
              'Name': host + '.' + self.zoneName,
              'Type': 'A',
              'TTL': self.ttl,
              'ResourceRecords': [{
                'Value': addr
              }]
            }
          }]
        }

        if action == 'DELETE' and self.lookupDNS(fqdn) == False:
          logger.debug("[ DELETE HOST RECORD ] Host record for " + fqdn + " does not exist and cannot be deleted")

        if self.lookupDNS(fqdn) == addr and action != 'DELETE':
          logger.debug("[ UPSERT HOST RECORD] DNS record for host " + fqdn + " already set to " + addr)
          return True
        try:
            res = self.aws.change_resource_record_sets(HostedZoneId=self.zoneId, ChangeBatch=batch)
            if action == 'Delete':
                logger.debug("[" +  action + " ] HOST RECORD Record for " + host + " has been deleted")
            else:
                logger.debug("[" +  action + " ] HOST RECORD Record for " + host + " updated to " + addr)
        except Exception as e:
            logger.debug("Could not " + action + " " + host + " A record:")
            logger.debug(e)
            return False


    """
    Add or remove the pod's A record to the multivalue record for the service
    """
    def updateServiceRecord(self, addr, host, action):
        logger.debug(addr + ' ' + host)
        if action == 'DELETE':
            batch = {
               'Changes': [{
                    'Action': action,
                    'ResourceRecordSet': {
                        'SetIdentifier': host,
                        'Name': self.serviceName,
                        'Type': 'A',
                        'TTL': self.ttl,
                        'ResourceRecords': [{
                            'Value': addr
                        }],
                        'MultiValueAnswer': True
                    }
                }]
              }
        else:
            batch = {
                'Changes': [{
                'Action': action,
                'ResourceRecordSet': {
                    'SetIdentifier': host,
                    'Name': self.serviceName,
                    'Type': 'A',
                    'TTL': self.ttl,
                    'ResourceRecords': [{
                        'Value': addr
                    }],
                    'MultiValueAnswer': True
                    }
                }]
            }
        try:
            res = self.aws.change_resource_record_sets(HostedZoneId=self.zoneId, ChangeBatch=batch)
            logger.debug("[ "  + action + " SERVICE RECORD ] Service record for " + host + " was updated")
            return res
        except Exception as e:
            if action == 'DELETE':
                logger.debug("[ DELETE SERVICE RECORD ] Deleting service record for pod " + host + "failed. The record may not have existed or their may have been an error in AWS")
                logger.debug(e)
                return False
            logger.debug(e)
            return False

    """
    Process a pod ADD/DELETE/MODIFY event
    """
    def processPodEvent(self, pod):
        for i in range(self.maxPodInfoRetries):
            podinfo = self.getPodInfo(pod["object"])
            if not podinfo:  # Pod is probably just not ready yet. Wait a while
                time.sleep(2)
            else:
                break
        if not podinfo: # We have hit our max number of tries and will give up
            self.unlockIt(pod["object"].metadata.name)
            return False        
        logger.debug("[ WATCH ] Pod " + str(podinfo["host"]) + " " + str(pod["type"]))
        if pod["type"] in ['ADDED', 'MODIFIED']:
          action = 'UPSERT'
        elif pod["type"] == 'DELETED':
          action = 'DELETE'
        else:
            action = 'UPSERT'
        try:
            self.updateHostRecord(podinfo['address'], podinfo['host'], action)
        except:
            pass
        try:
            self.updateServiceRecord(podinfo['address'], podinfo['host'], action)
        except:
            pass
        self.unlockIt(pod["object"].metadata.name)

    """
    Get a lock on the list of pods currently being processed and add our pod
    """
    def lockIt(self, pod):
        test = lock.acquire(False)
        if test:
            #logger.debug("Got a lock on " + pod)
            if pod not in podsInProcess:
                podsInProcess.append(pod)
                try:
                    lock.release()
                except:
                    pass
                return True
            else:
                logger.debug("Pod " + pod + " is already being processed.")
                return False
        else:
            try:
                lock.release()
            except:
                pass
            return False

    """
    Get a lock on the list of pods currently being processed and remove our pod
    """
    def unlockIt(self, pod):
        test = lock.acquire(False)
        if test:
            #logger.debug("Got a lock on " + pod)
            if pod in podsInProcess:               
                podsInProcess.remove(pod)
                try:
                    lock.release()
                except:
                    pass
                #logger.debug("Lock released")
                return True
            else:
                try:
                    lock.release()
                except:
                    pass
                #logger.debug("Lock released")
                return True
        else:
            try:
                lock.release()
            except:
                pass
            return True

    def watchPods(self):
        logger.debug("Watching pods")
        w = watch.Watch()
        for item in w.stream(self.kube.list_namespaced_pod, namespace=self.namespace, label_selector=self.label_selector, timeout_seconds=0):
            while item["object"].metadata.name in podsInProcess:
                logger.debug("[ WATCH ] Waiting to acquire lock on " + item["object"].metadata.name)
                time.sleep(1)
            t = Thread(target=self.processPodEvent(item))
            t.daemon = True
            t.start()

    def checkZoneForOrphanServiceRecords(self):
        allRecords = self.aws.list_resource_record_sets( HostedZoneId=self.zoneId, StartRecordName=self.serviceName, StartRecordType='A')
        records = []
        # Filter down to only the records that are part of the service
        for record in allRecords["ResourceRecordSets"]:
            if record["Name"].rstrip('.') == self.serviceName and "SetIdentifier" in record:
               newRecord = {
                            "pod": record["SetIdentifier"],
                            "ip": record["ResourceRecords"][0]["Value"]
                           }
               records.append(newRecord)
        pods = []
        # Get a list of current pod names
        for pod in self.getPodsByLabel():
            podinfo = self.getPodInfo(pod)
            if podinfo:
                pods.append(podinfo["host"])
        # Check if the record actually has an existing pod that matches its SetIdentifier
        for record in records:
            if record["pod"] not in pods and record["pod"] and self.lockIt(record["pod"]) != False:
                logger.debug("[ CHECK FOR ORPHAN SERVICE RECORD] A service record for pod " + record["pod"] + " exists but the pod does not. Attempting to remove.")
                self.updateServiceRecord(record["ip"], record["pod"], 'DELETE')
                unlockIt(record["pod"])

    def checkZoneForOrphanHostRecords(self):
        allRecords = self.aws.list_resource_record_sets( HostedZoneId=self.zoneId, StartRecordName=self.serviceName, StartRecordType='A')
        records = []
        # Filter down to records that are only for our direct hosts
        for record in allRecords["ResourceRecordSets"]:
            if record["Type"] == 'A' and "SetIdentifier" not in record and re.search("^" + self.app + "-.*\." + self.zoneName, record["Name"]) != None:
                records.append({"name": record["Name"].rstrip(".").replace("." + self.zoneName, ''), "ip": record["ResourceRecords"][0]["Value"]})
        pods = []
        # Get a list of current pod names
        for pod in self.getPodsByLabel():
            podinfo = self.getPodInfo(pod)
            if podinfo:
                pods.append(podinfo["host"])
        for record in records:
            if record["name"] not in pods and record["name"] and self.lockIt(record["name"]) != False:
                self.updateHostRecord(record["ip"], record["name"], 'DELETE')
                self.unlockIt(record["name"])

    """
    Return a thread of watchPods method that watches for pod events via the kube API
    """
    def runWatchPods(self):
        t = Thread(name="watchPods", target=self.watchPods)
        t.daemon = True
        t.start()
        return t

    """
    Check for orphaned records in a loop
    """
    def monitorForOrphanRecords(self):
        while True:
            logger.debug("Checking for orphaned records")
            self.checkZoneForOrphanHostRecords()
            self.checkZoneForOrphanServiceRecords()
            time.sleep(self.iterationDelay)

    """
    Return a thread that watches for orphaned records in a loop
    """
    def runMonitorForOrphanRecords(self):
        t = Thread(name="monitorForOrphanRecords", target=self.monitorForOrphanRecords)
        t.daemon = True
        t.start()
        return t

    """
    Loops and monitors pods looking for pods that somehow didn't get their records created/updated
    """
    def monitorForMissingRecords(self):
        while True:
            logger.debug("Checking for missing records")
            self.updatePodDNS()
            time.sleep(self.iterationDelay)

    """
    Return a thread that monitors for pod records that should exist but don't
    """
    def runMonitorForMissingRecords(self):
        t = Thread(name="monitorForMissingRecords", target=self.monitorForMissingRecords)
        t.daemon = True
        t.start()
        return t


    def runAll(self):
        pp("#################################################################################################################")
        pp("Starting DNS service with the following options:")
        pp("    Domain = " + self.domain)
        pp("    DNS Zone Name = " + self.zoneName)
        pp("    AWS Zone ID = " + self.zoneId) 
        pp("    Service Name = " + self.serviceName)
        pp("    Pod Namespace = " + self.namespace)
        pp("    TTL for records = " + str(self.ttl))
        pp("    App Name = " + self.app)
        pp("    Node Selector = " + self.node_selector)
        pp("    Pod label selector = " + self.label_selector)
        pp("    Iteration delay = " + str(self.iterationDelay))
        pp("    Max retries for getting pod info = " + str(self.maxPodInfoRetries))
        pp("#################################################################################################################")

        watchPods = self.runWatchPods()
        time.sleep(math.floor(self.iterationDelay / 2))
        orphans = self.runMonitorForOrphanRecords()
        time.sleep(math.floor(self.iterationDelay / 2))
        missing = self.runMonitorForMissingRecords()

        while True:
            if not watchPods.isAlive():
                logger.debug("Thread 'watchPods' has died")
                raise SystemExit(1)
            if not orphans.isAlive():
                logger.debug("Thread 'watchForOrphans' has died")
                raise SystemExit(1)
            if not missing.isAlive():
                logger.debug("Thread 'watchForMissing' has died")
                raise SystemExit(1)
            time.sleep(1)
