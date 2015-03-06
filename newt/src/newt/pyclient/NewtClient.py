
import os
import os.path

from ConfigParser import ConfigParser
from xmlrpclib import ServerProxy
from xmlrpclib import Error as RPCError

class NewtClient(object):
    def __init__(self):
        self.config = ConfigParser()

        self.config.read('./conf/newt.py.cfg')
        self.masterURL = 'http://%s:%s/xmlrpc' % \
            (self.config.get('Newt', 'masterHostName'),
             self.config.get('Newt', 'masterPort'))

        self.logger = NewtLogger(self.config.get('Newt', 'clientLogDir'))

        # KCW: Magic number?
        self.maxProv = 500

        self.inputVectors = {}
        self.outputVectors = {}
        self.actorTables = {}
        self.tableSchemas = {}
        self.actorConnections = {}

        print 'NewtClient constructor, Python style!'
        os.stdout.flush()

        self.client = self.connectClient(-1, self.masterURL)

    def connectClient(self, actorID, url)
        try:
            self.client = ServerProxy(url)
        except RPCError as e:
            print e

        print 'Changed connectClient'

        self.actorConnections[actorID] = self.client

        return self.client

    def getUniverse(self, uname):
        try:
            uid = self.client.Newt.getUniverse(uname)
        except RPCError as e:
            print e

        print 'Returning uid: %d for universe: %s' % (uid, uname)

    def register(self, aname, parentID):
        try:
            aid = self.client.Newt.register(aname, parentID)
        except RPCError as e:
            print e

        print 'Returning rood aid: %d for root actor: %s with parent: %d' % \
            (aid, aname, parentID)

        return aid

    def addProvenance(self, actorID, tableID, schemaID, inputs, outputs):
        print 'Writing provenance for %d with table name: %s' % \
            (actorID, tableID)

        try:
            aUrl = self.client.Newt.getProvenanceNode(actorID, tableID, schemaID)
        except RPCError as e:
            print e

        self.connectClient(actorID, aUrl)

        # If seeing this tableID for the first time
        if tableID not in self.inputVectors:
            self.inputVectors[tableID] = []
            self.outputVectors[tableID] = []
            self.tableSchemas[tableID] = schemaID

            # If seeing this actorID for the first time
            if actorID not in self.actorTables:
                self.actorTables[actorID] = []

            self.actorTables[actorID].append(tableID)

        pInputs = ''.join(inputs) + '\r\n'
        #XXX encoding?
        #self.inputVectors[tableID].append(?)
        self.inputVectors[tableID].append(
            array('c', pInputs.encode('latin_1')))

        pOutputs = ''.join(outputs) + '\r\n'
        #XXX encoding?
        #self.outputVectors[tableID].append(?)
        self.outputVectors[tableID].append(
            array('c', pOutputs.encode('latin_1')))

        if len(self.inputVectors) >= self.maxProv:
            self.sendProvenance(actorID, tableID)

    def sendProvenance(self, actorID, tableID):
        try:
            result = self.actorConnections[actorID].Newt.addProvenance(actorID,
                tableID, self.tableSchemas[tableID], self.inputVectors[tableID],
                self.outputVectors[tableID])
        except RPCError as e:
            print e
            self.logger.log(actorID, tableID, self.tableSchemas[tableID],
                self.inputVectors[tableID], self.outputVectors[tableID])

        return result

    def getID(self, aname, atype):
        try:
            aid = self.client.Newt.getID(aname, atype)
        except RPCError as e:
            print e

        return aid

    def addActor(self, parentID, aname, atype):
        try:
            aid = self.client.Newt.addActor(parentID, aname, atype)
        except RPCError as e:
            print e

        print 'Returning aid: %d for actor: %s with parent %d' % \
            (aid, aname, parentID)

        return aid

    def setProvenanceHierarchy(self, actorID, hierarchy):
        try:
            result = self.client.Newt.setProvenanceHierarchy(actorID, hierarchy)
        except RPCError as e:
            print e

        return result

    def setProvenanceSchemas(self, actorID, schemas):
        try:
            result = self.client.Newt.setProvenanceSchemas(actorID, schemas)
        except RPCError as e:
            print e

        print 'Setting provenance schemas.'

        return result

    def getSchemaID(self, uID, schemaName):
        try:
            sid = self.client.Newt.getSchemaID(uID, schemaName)
        except RPCError as e:
            print e

        print 'Returning schemaID: %d for schemaName: %s' % (sid, schemaName)

        return sid

    def commit(self, actorID):
        print 'Commiting: %d' % actorID

        for tableID in self.actorTables[actorID]:
            self.sendProvenance(actorID, tableID)

        # KCW: Why loop a second time over the same list?
        for tableID in self.actorTables[actorID]:
            self.logger.makeAttempt(actorID, tableID,
                self.tableSchemas[tableID], self.actorConnections[actorID])

        try:
            result = self.client.Newt.commit(actorID)
        except RPCError as e:
            print e

    def trace(self, data, direction, containingID):
        try:
            tid = self.client.Newt.trace(data, direction, containingID)
        except RPCError as e:
            print e

        return tid

    def getTraceResults(self, tid, aname, atype):
        aid = self.getID(aname, atype)

        try:
            aUrl = self.client.Newt.getTraceNode(aid)
        except RPCError as e:
            print e

        try:
            newClient = ServerProxy(self.masterURL)
        except RPCError as e:
            print e

        print 'Changed get traceResults'

        try:
            b = newClient.Newt.getLocalTraceResults(aid, tid)
        except RPCError as e:
            print e

        results = []

        #XXX byte encoding?  What type is b?
        # Convert b to string, split on '\r\n', convert to bytes, add to results

        return results

class NewtStageClient(object):
    def __init__(self, newtClient):
        self.newtClient = newtClient
        self.inputBuffer = []
        self.trgr = None

    #KCW: renamed "input" to iput, since input has a built-in meaning in Python
    def addInput(self, actorID, tableName, schemaID, trigger, iput):
        if trigger == self.trgr:
            self.inputBuffer.append(iput)
        else:
            print 'Clearing trgr: [%s] and replacing by: %s' % (self.trgr,
                trigger)
            self.inputBuffer = [iput]

        print 'Statging input: [%s] for actorID: %d' % (iput, actorID)

        return 0

    def addOutput(self, actorID, tableName, schemaID, output):
        print 'Flushing output: [%s] for actorID: %d' % (output, actorID)

        i = []
        o = [output]

        #KCW: I don't understand at all why we add/clear the list for every
        #element, but this is a literal translation...
        for iput in self.inputBuffer:
            i.append(iput)
            self.newtClient.addProvenance(actorID, tableName, schemaID, i, o)
            i = []

        return 0

class NewtLogger(object):
    def __init__(self, logdir):
        if not os.path.exists(logdir):
            os.mkdir(logdir)

        self.logdir = logdir

    def log(self, actorID, tableID, schemaID, inputs, outputs):
        f = open('%s/%d_%s_%d.log' % (self.logdir, actorID, tableID, schemaID),
            'w')

        for i in xrange(len(inputs)):
            inputList = inputs[i].replace('\r\n', ',')
            f.write(inputList + '\t')

            outputList = outputs[i].replace('\r\n', ',')
            f.write(outputList + '\n')

        f.close()

    def makeAttempt(self, actorID, tableID, schemaID, client):
        inputVector = []
        outputVector = []

        f = open('%s/%d_%s_%d.log' % (self.logdir, actorID, tableID, schemaID),
            'r')

        for line in f:
            io = line.split('\t')
            inputs = io[0].replace(',', '\r\n')
            outputs = io[1].replace(',', '\r\n')
            inputVector.append(inputs)
            outputVector.append(outputs)

        try:
            result = client.Newt.addProvenance(actorID, tableID, schemaID,
                inputVector, outputVector)
        except RPCError as e:
            print e
