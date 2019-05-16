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
    """
    Provides tools to watch the kube API for events on namespaced and labeled pods scheduled on labeled nodes
    Configuration is expected to be provided via environment variables and takes no parameters to the constructor

    Attributes
    -----------
    domain : str
        The domain that AWS records will be created in. This may be different than zoneName
    serviceName : str
        The multivalue DNS record name that all hosts will have an entry for. A type of service discovery.
    namespace : str
        The kube namespace that we will monitor for pods
    zoneId : str
        The AWS zone id for the DNS zone that all records will belong to
    zoneName : str
        The name of the DNS zone to use. May not necessarily be the same as the domain
    ttl : int
        The ttl to use with records. Note that the AWS client will not be able to update any existing records that have a different TTL
    node_selector : str
        The label to use for targeting nodes
    label_selector : str
        The label to use for targeting pods
    iterationDelay : int
        The time to wait between iterations when scanning AWS for orphaned records or scanning pods for mismatched records
    maxPodInfoRetries : int
        The max number of times to retry getting a lock on a pod before giving up getting the pod's info for processing. If a lock is not acquired,
        and there runMonitorForOrphanRecords() and runMonitorForMissingRecords() are running then you should still catch the missing/orphaned records.

    Methods
    -------
        getNodeIP()
        getPodsByLabel()
        updatePodDNS()
        getPodInfo()
        lookupDNS()
        updateHostRecord()
        updateServiceRecord()
        processPodEvent()
        lockIt()
        unlockIt()
        watchPods()
        checkZoneForOrphanServiceRecords()
        checkZoneForOrphanHostRecords()
        runWatchPods()
        monitorForOrphanRecords()
        runMonitorForOrphanRecords()
        monitorForMissingRecords()
        runMonitorForMissingRecords()
        banner
        runAll()
    """

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
    lockedBy = ""

    def __init__(self):
        # Initialize Kube client. Config and credentials are expected to be provided via the pod itself and uses RBAC
        config.load_incluster_config()

        # Turns on DEBUG for the Kube library
        if "DEBUG" in os.environ and os.environ["DEBUG"]:
            config.debug = True

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
        self.app = os.environ['APP_NAME']
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


    def getNodeIP(self, node):
        """
        Check if the public ip of a node matches the ip arg

        Args:
            node (str): Name of node to get the public IP of

        Returns:
            bool: bool on exception
            str: if successful will return the public IP of the node as a string
        """

        try:
            nodes = self.kube.list_node(label_selector=self.node_selector)
        except Exception as e:
            logger.debug("[ getNodeIP ] Could not get list of nodes:")
            logger.debug(e)
            return False
        for n in nodes.items:
            name = n.metadata.name
            if name == node:
                # Get the external node address
                for addr in n.status.addresses:
                    if addr.type == 'ExternalIP':
                        return addr.address


    def getPodsByLabel(self):
        """
        Get all pods in self.namespace labeled with self.label_selector

        Args: none

        Returns:
            pods (dict): A dictionary of pods and their metadata on success, empty dictionary on exception.
        """

        try:
            res = self.kube.list_namespaced_pod(namespace=self.namespace, label_selector=self.label_selector)
            return res.items
        except Exception as e:
            logger.debug("[ getPodsByLabel ] Could not list namespaced pods: ")
            logger.debug(e)
            return []


    def updatePodDNS(self):
        """
        Iterates the list pods obtained from self.getPodsByLabel() and updates their DNS records to match their current state

        Args: none

        Returns: none
        """

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
                if not ip or not nodeIP:
                    logger.debug("Node IP does not match external DNS record for pod: Pod IP = " + fqdn)
                else:
                    logger.debug("Node IP does not match external DNS record for pod: Pod IP = " + ip + " Node IP = " + nodeIP)
                try:
                    logger.debug("Trying to update pod DNS")
                    hostDNS = self.updateHostRecord(nodeIP, pod.metadata.name, 'UPSERT')
                except Exception as e:
                    logger.debug("Updating pod " + pod.metadata.name + " Host DNS failed")
                    logger.debug(e)
            self.unlockIt(pod.metadata.name)

                # Check the service record and update if necessary
        for pod in self.getPodsByLabel():
            if not self.lockIt(pod.metadata.name):
                continue
            fqdn = pod.metadata.name + '.' + self.zoneName
            ip = self.lookupDNS(fqdn)
            node = str(pod.spec.node_name)
            nodeIP = self.getNodeIP(node)
            try:
                records = self.aws.list_resource_record_sets(HostedZoneId=self.zoneId, StartRecordName=self.serviceName, StartRecordType='A')
            except Exception as e:
                logger.debug("[ UPDATE POD DNS ] ")
                logger.debug(e)
                self.unlockIt(pod.metadata.name)
                continue
            for record in records["ResourceRecordSets"]:
                if record["Name"].rstrip('.') == self.serviceName and "SetIdentifier" in record and record["SetIdentifier"] == pod.metadata.name:
                    if record["ResourceRecords"][0]["Value"] != nodeIP:
                        try:
                            serviceDNS = self.updateServiceRecord(nodeIP, pod.metadata.name, 'UPSERT')
                        except Exception as e:
                            logger.debug("Updating pod " + pod.metadata.name + " Service DNS failed.")
                            logger.debug(e)

            self.unlockIt(pod.metadata.name)


    def getPodInfo(self, pod):
        """
        Gets pod's public address from its node and returns the pod name and IP

            Args:
                pod (obj): a kubernetes client pod object

            Returns:
                bool: On exception or if the pod has no node name in its metadata (Normally means the pod has not finished being scheduled yet and is rare).
                dict: On success returns a dictionary containing the pod's external address from the node, pod's external hostname, and name of the node it is scheduled on.
        """

        podHostname = pod.metadata.name
        try:
            nodeName = pod.spec.node_name
            if not nodeName:
                logger.debug("[ Get Pod INFO ] Pod " + podHostname + " not assigned to a node yet.")
                return False
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
                if not i.address:
                    logger.debug("[ getPodInfo ] Could not get external address for pod on host " + nodeName)
                    return False
                return {"address": i.address, "host": podHostname, "nodeName": nodeName}


    def lookupDNS(self, host):
        """
        Tests DNS records so we can return from the caller without doing any unneccessary work

        Args:
            host (str): The DNS hostname to test

        Returns:
            bool: If the hostname is not resolvable
            str: IP address of the hostname if the address can be resolved
        """

        r = dns.resolver.Resolver(configure=False)
        # TODO: Make resolvers configurable
        r.nameservers = ["8.8.8.8", "8.8.4.4"]
        try:
            test = r.query(host)
            return str(test[0])
        except Exception as e:
            return False


    def updateHostRecord(self, addr, host, action):
        """
        Updates, creates, or deletes an A record for a single pod.
        Updates are done with UPSERT, deletes with DELETE action in data sent to AWS

        Args:
            addr (str): IP address for the new record.
            host (str): The FQDN for the host's record.
            action: (str): UPDATE || DELETE the host's record

        Returns:
            bool: Return False on exception, if record is already correct, or if we are trying to DELETE and the record does not exist
            obj: Return the AWS response object if we are successfull

        """

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
            return False
        if self.lookupDNS(fqdn) == addr and action != 'DELETE':
            logger.debug("[ UPSERT HOST RECORD] DNS record for host " + fqdn + " already set to " + addr)
            return False
        try:
            res = self.aws.change_resource_record_sets(HostedZoneId=self.zoneId, ChangeBatch=batch)
            # TODO: Check response for status and don't return until the operation has finished
            if action == 'Delete':
                logger.debug("[" +  action + " ] HOST RECORD Record for " + host + " has been deleted")
                return res
            else:
                logger.debug("[" +  action + " ] HOST RECORD Record for " + host + " updated to " + addr)
                return res
        except Exception as e:
            logger.debug("Could not " + action + " " + host + " A record:")
            logger.debug(e)
            return False


    def updateServiceRecord(self, addr, host, action):
        """
        Add or remove the pod's A record to the multivalue record for the service

        Args:
            addr (str): IP address for the new record.
            host (str): The FQDN for the host's record.
            action: (str): UPDATE || DELETE the host's record

        Returns:
            bool: Return False on exception, if record is already correct, or if we are trying to DELETE and the record does not exist
            obj: Return the AWS response object if we are successfull
        """

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
            # TODO: Check response for status and don't return until the operation has finished
            return res
        except Exception as e:
            if action == 'DELETE':
                logger.debug("[ DELETE SERVICE RECORD ] Deleting service record for pod " + host + "failed. The record may not have existed or their may have been an error in AWS")
                logger.debug(e)
                return False
            logger.debug(e)
            return False


    def processPodEvent(self, pod):
        """
        Process a pod ADD/DELETE/MODIFY event
            Args:
                pod (obj): Pod object returned by the kubernetes API

            Returns:
                none
        """
        # TODO: return something
        for i in range(self.maxPodInfoRetries):
            podinfo = self.getPodInfo(pod["object"])
            if podinfo:
                break
            if not podinfo and i == (self.maxPodInfoRetries - 1):  # Pod is probably just not ready yet. Wait a while
                logger.debug("[ PROCESS POD EVENT ] Ran out of retries waiting for pod " + pod["object"].metadata.name + " to enter READY state." )
                break
            else:
                logger.debug("[ PROCESS POD EVENT ] Waiting for pod to become ready")
                time.sleep(2)
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
            res = self.updateHostRecord(podinfo['address'], podinfo['host'], action)
        except Exception as e:
            logger.debug("[ updateHostRecord ] Could not update record: ")
            logger.debug(e)
        try:
            self.updateServiceRecord(podinfo['address'], podinfo['host'], action)
        except Exception as e:
            logger.debug("[ updateServiceRecord ] Could not update record: ")
            logger.debug(e)
        self.unlockIt(pod["object"].metadata.name)


    def lockIt(self, pod):
        """
        Get a lock on the list of pods currently being processed and add our pod without waiting if it is already locked
            Args:
                pod (str): name of pod to lock

            Returns:
                bool: True on successfull lock. False otherwise

        """

        if lock.acquire(False):
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


    def unlockIt(self, pod):
        """
        Get a lock on the list of pods currently being processed and remove our pod

            Args:
                pod (str): name of pod to unlock

            Returns:
                bool: True on successfull unlock. False otherwise

        """

        if pod in podsInProcess:
            try:
                podsInProcess.remove(pod)
            except:
                pass
            try:
                lock.release()
            except:
                pass
            return True

    def watchPods(self):
        """
        Set a watch on the kube API for pod events targeted by pod namespace and labels

            Args:
                none

            Returns:
                none
        """

        logger.debug("Watching pods")
        w = watch.Watch()
        try:
            for item in w.stream(self.kube.list_namespaced_pod, namespace=self.namespace, label_selector=self.label_selector, timeout_seconds=0):
                for i in range(self.maxPodInfoRetries):
                    logger.debug("[ WATCH ] Acquiring lock try " + str(i + 1) + " of " + str(self.maxPodInfoRetries))
                    if self.lockIt(item["object"].metadata.name):
                        t = Thread(target=self.processPodEvent(item))
                        t.daemon = True
                        t.start()
                        break
                    else:
                        if i == (self.maxPodInfoRetries - 1):
                            logger.debug("[ WATCH ] Ran out of retries to acquire lock on " + item["object"].metadata.name + " for " + str(item["type"]) + " operation")
                        else:
                            logger.debug("[ WATCH ] Waiting to acquire lock on " + item["object"].metadata.name + " for " + str(item["type"]) + " operation")
                            time.sleep(1)
        except Exception as e:
            logger.debug("[ watchPods ] ERROR:")
            logger.debug(e)


    def checkZoneForOrphanServiceRecords(self):
        """
        Iterate through all of the self.serviceName entries from AWS and make sure there is a pod that matches that record. Delete the record if not

            Args:
                none

            Returns:
                none
        """

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
                self.unlockIt(record["pod"])


    def checkZoneForOrphanHostRecords(self):
        """
        Iterate through all of the records in our zone that match <pod regex>.servicename and remove any orphans

            Args:
                none

            Returns:
                none
        """

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


    def runWatchPods(self):
        """
        Return a thread of watchPods() method that watches for pod events via the kube API

            Args:
                none

            Returns:
                none
        """

        t = Thread(name="watchPods", target=self.watchPods)
        t.daemon = True
        t.start()
        return t


    def monitorForOrphanRecords(self):
        """
        Check for orphaned records in a loop

            Args:
                none

            Returns:
                none
        """
        while True:
            logger.debug("Checking for orphaned records")
            self.checkZoneForOrphanHostRecords()
            self.checkZoneForOrphanServiceRecords()
            time.sleep(self.iterationDelay)


    def runMonitorForOrphanRecords(self):
        """
        Return a thread that watches for orphaned records in a loop

            Args:
                none

            Returns:
                none
        """

        t = Thread(name="monitorForOrphanRecords", target=self.monitorForOrphanRecords)
        t.daemon = True
        t.start()
        return t


    def monitorForMissingRecords(self):
        """
        Loops and monitors pods looking for pods that somehow didn't get their records created/updated

            Args:
                none

            Returns:
                none
        """

        while True:
            logger.debug("Checking for missing records")
            self.updatePodDNS()
            time.sleep(self.iterationDelay)


    def runMonitorForMissingRecords(self):
        """
        Return a thread that monitors for pod records that should exist but don't

            Args:
                none

            Returns:
                none
        """

        t = Thread(name="monitorForMissingRecords", target=self.monitorForMissingRecords)
        t.daemon = True
        t.start()
        return t


    def banner(self):
        string = """
        #################################################################################################################
            Starting DNS service with the following options:
                Domain = {domain}
                DNS Zone Name = {zonename}
                AWS Zone ID = {zoneid}
                Service Name = {service}
                Pod Namespace = {namespace}
                TTL for records = {ttl}
                App Name = {app}
                Node Selector = {node}
                Pod label selector = {label}
                Iteration delay = {delay}
                Max retries for getting pod info = {retries}
        #################################################################################################################
        """.format(
                domain=self.domain,
                zonename=self.zoneName,
                zoneid=self.zoneId,
                service=self.serviceName,
                namespace=self.namespace,
                ttl=str(self.ttl),
                app=self.app,
                node=self.node_selector,
                label=self.label_selector,
                delay=str(self.iterationDelay),
                retries=str(self.maxPodInfoRetries)
            )
        print(string)


    def runAll(self):
        """
        Start threads that watch for ADD/DELETE/MODIFY pod events, scan for orphan service records, and scans for orphan pod records.
        Babysit the threads and exit on anything other than runWatchPods() exiting (just restart that thread)
            Args:
                none

            Returns:
                none
        """

        self.banner()
        time.sleep(2)
        watchPods = self.runWatchPods()
        time.sleep(math.floor(self.iterationDelay / 2))
        orphans = self.runMonitorForOrphanRecords()
        time.sleep(math.floor(self.iterationDelay / 2))
        missing = self.runMonitorForMissingRecords()

        while True:
            if not watchPods.isAlive():
                # TODO: Find out why Watch() exits without error at times
                logger.debug("Thread 'watchPods' has died")
                logger.debug("Restarting Watch().stream()")
                watchPods = self.runWatchPods()
            if not orphans.isAlive():
                logger.debug("Thread 'watchForOrphans' has died")
                raise SystemExit(1)
            if not missing.isAlive():
                logger.debug("Thread 'watchForMissing' has died")
                raise SystemExit(1)
            time.sleep(1)
