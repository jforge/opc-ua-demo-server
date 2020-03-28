package com.digitalpetri.opcua.server.namespaces.demo

import com.digitalpetri.opcua.milo.extensions.defaultValue
import com.digitalpetri.opcua.milo.extensions.inverseReferenceTo
import com.digitalpetri.opcua.milo.extensions.resolve
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.*
import org.eclipse.milo.opcua.sdk.core.AccessLevel
import org.eclipse.milo.opcua.sdk.core.Reference
import org.eclipse.milo.opcua.sdk.server.AbstractLifecycle
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.UaNodeManager
import org.eclipse.milo.opcua.sdk.server.api.*
import org.eclipse.milo.opcua.sdk.server.api.methods.MethodInvocationHandler
import org.eclipse.milo.opcua.sdk.server.api.services.AttributeServices.ReadContext
import org.eclipse.milo.opcua.sdk.server.api.services.AttributeServices.WriteContext
import org.eclipse.milo.opcua.sdk.server.api.services.MethodServices
import org.eclipse.milo.opcua.sdk.server.api.services.MethodServices.CallContext
import org.eclipse.milo.opcua.sdk.server.api.services.ViewServices.BrowseContext
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.ServerTypeNode
import org.eclipse.milo.opcua.sdk.server.nodes.*
import org.eclipse.milo.opcua.sdk.server.nodes.factories.NodeFactory
import org.eclipse.milo.opcua.sdk.server.nodes.filters.AttributeFilter
import org.eclipse.milo.opcua.sdk.server.nodes.filters.AttributeFilterContext.GetAttributeContext
import org.eclipse.milo.opcua.sdk.server.nodes.filters.AttributeFilterContext.SetAttributeContext
import org.eclipse.milo.opcua.stack.core.*
import org.eclipse.milo.opcua.stack.core.types.builtin.*
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ushort
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn
import org.eclipse.milo.opcua.stack.core.types.structured.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

class DemoNamespace(
    internal val server: OpcUaServer,
    private val coroutineScope: CoroutineScope
) : AbstractLifecycle(), Namespace {

    companion object {
        const val NAMESPACE_URI: String = "urn:eclipse:milo:opcua:server:demo"
    }

    private val logger: Logger = LoggerFactory.getLogger(DemoNamespace::class.java)

    internal val nodeManager = UaNodeManager()

    internal val nodeContext = object : UaNodeContext {
        override fun getServer() =
            this@DemoNamespace.server

        override fun getNodeManager() =
            this@DemoNamespace.nodeManager
    }

    internal val nodeFactory = NodeFactory(nodeContext)

    internal lateinit var dictionaryManager: DataTypeDictionaryManager

    private lateinit var eventFuture: ScheduledFuture<*>

    private val monitoringJobs: ConcurrentMap<DataItem, Job> = Maps.newConcurrentMap()

    private val namespaceIndex: UShort = server.namespaceTable.addUri(NAMESPACE_URI)

    private val filter = SimpleAddressSpaceFilter.create {
        it.namespaceIndex == namespaceIndex
    }

    override fun onStartup() {
        server.addressSpaceManager.register(this)
        server.addressSpaceManager.register(nodeManager)

        dictionaryManager = DataTypeDictionaryManager(
            nodeContext,
            NAMESPACE_URI
        )

        dictionaryManager.startup()

        addCttNodes()
        addMassNodes()
        addTurtleNodes()
        addFileNodes()
        addDemoMethodNodes()
        addDynamicNodes()
        addNullValueNodes()
        addComplexTypeNodes()

        // Set the EventNotifier bit on Server Node for Events.
        val serverNode = server.addressSpaceManager.getManagedNode(Identifiers.Server).orElse(null)

        if (serverNode is ServerTypeNode) {
            serverNode.eventNotifier = ubyte(1)

            // Post a bogus Event every couple seconds
            eventFuture = server.scheduledExecutorService.scheduleAtFixedRate({
                try {
                    val eventNode = server.eventFactory.createEvent(
                        NodeId(1, UUID.randomUUID()),
                        Identifiers.BaseEventType
                    )

                    eventNode.browseName = QualifiedName(1, "foo")
                    eventNode.displayName = LocalizedText.english("foo")

                    eventNode.eventId = ByteString.of(byteArrayOf(0, 1, 2, 3))
                    eventNode.eventType = Identifiers.BaseEventType
                    eventNode.sourceNode = serverNode.getNodeId()
                    eventNode.sourceName = serverNode.getDisplayName().text
                    eventNode.time = DateTime.now()
                    eventNode.receiveTime = DateTime.NULL_VALUE
                    eventNode.message = LocalizedText.english("event message!")
                    eventNode.severity = ushort(2)

                    @Suppress("UnstableApiUsage")
                    server.eventBus.post(eventNode)

                    eventNode.delete()
                } catch (e: Throwable) {
                    logger.error("Error creating EventNode: {}", e.message, e)
                }
            }, 0, 2, TimeUnit.SECONDS)
        }
    }

    override fun onShutdown() {
        eventFuture.cancel(true)

        monitoringJobs.values.forEach { it.cancel() }

        dictionaryManager.shutdown()

        server.addressSpaceManager.unregister(this)
        server.addressSpaceManager.unregister(nodeManager)
    }

    override fun getFilter(): AddressSpaceFilter {
        return filter
    }

    override fun getNamespaceUri(): String = NAMESPACE_URI

    override fun getNamespaceIndex(): UShort = this@DemoNamespace.namespaceIndex

    override fun browse(context: BrowseContext, viewDescription: ViewDescription, nodeId: NodeId) {
        val node: UaNode? = nodeManager[nodeId]

        val references: List<Reference>? = node?.references ?: maybeTurtleReferences(nodeId)

        if (references != null) {
            context.success(references)
        } else {
            context.failure(StatusCodes.Bad_NodeIdUnknown)
        }
    }

    override fun getReferences(
        context: BrowseContext,
        viewDescription: ViewDescription,
        sourceNodeId: NodeId
    ) {

        val references: List<Reference> =
            nodeManager.getReferences(sourceNodeId) +
                (maybeTurtleReferences(sourceNodeId) ?: emptyList())

        context.success(references)
    }

    override fun read(
        context: ReadContext,
        maxAge: Double,
        timestamps: TimestampsToReturn,
        readValueIds: List<ReadValueId>
    ) {

        val values = readValueIds.map { readValueId ->
            val node: UaNode? = nodeManager[readValueId.nodeId] ?: maybeTurtleNode(readValueId.nodeId)

            val value: DataValue? = node?.readAttribute(
                AttributeContext(context),
                readValueId.attributeId,
                timestamps,
                readValueId.indexRange,
                readValueId.dataEncoding
            )

            value ?: DataValue(StatusCodes.Bad_NodeIdUnknown)
        }

        context.success(values)
    }

    override fun write(context: WriteContext, writeValues: List<WriteValue>) {
        val results: List<StatusCode> = writeValues.map { writeValue ->
            val node: UaNode? = nodeManager[writeValue.nodeId]

            val status: StatusCode? = node?.run {
                try {
                    writeAttribute(
                        AttributeContext(context),
                        writeValue.attributeId,
                        writeValue.value,
                        writeValue.indexRange
                    )
                    StatusCode.GOOD
                } catch (e: UaException) {
                    e.statusCode
                }
            }

            status ?: StatusCode(StatusCodes.Bad_NodeIdUnknown)
        }

        context.success(results)
    }

    override fun onCreateDataItem(
        itemToMonitor: ReadValueId,
        requestedSamplingInterval: Double,
        requestedQueueSize: UInteger,
        revisionCallback: BiConsumer<Double, UInteger>
    ) {

        if (itemToMonitor.nodeId.isMassNode()) {
            revisionCallback.accept(0.0, requestedQueueSize)
        } else {
            super.onCreateDataItem(itemToMonitor, requestedSamplingInterval, requestedQueueSize, revisionCallback)
        }
    }

    override fun onModifyDataItem(
        itemToModify: ReadValueId,
        requestedSamplingInterval: Double,
        requestedQueueSize: UInteger,
        revisionCallback: BiConsumer<Double, UInteger>
    ) {

        if (itemToModify.nodeId.isMassNode()) {
            revisionCallback.accept(0.0, requestedQueueSize)
        } else {
            super.onModifyDataItem(itemToModify, requestedSamplingInterval, requestedQueueSize, revisionCallback)
        }
    }

    @ExperimentalCoroutinesApi
    override fun onDataItemsCreated(items: List<DataItem>) {
        items.forEach { item -> startMonitoringItem(item) }
    }

    @ExperimentalCoroutinesApi
    override fun onDataItemsModified(items: List<DataItem>) {
        items.forEach {
            stopMonitoringItem(it)
            startMonitoringItem(it)
        }
    }

    override fun onDataItemsDeleted(items: List<DataItem>) {
        items.forEach { stopMonitoringItem(it) }
    }

    @ExperimentalCoroutinesApi
    private fun startMonitoringItem(item: DataItem) {
        val nodeId: NodeId = item.readValueId.nodeId
        val node: UaNode? = nodeManager.get(nodeId)

        if (node != null) {
            if (nodeId.isMassNode()) {
                monitoringJobs[item] = coroutineScope.monitorByCallback(node, item)
            } else {
                monitoringJobs[item] = coroutineScope.monitorBySampling(node, item)
            }
        }
    }

    private fun stopMonitoringItem(item: DataItem) {
        monitoringJobs.remove(item)?.cancel()
    }

    @ExperimentalCoroutinesApi
    override fun onMonitoringModeChanged(items: List<MonitoredItem>) {
        // no action needed; monitoring Jobs check isSamplingEnabled
    }

    private fun NodeId.isMassNode(): Boolean {
        val id = identifier as? UInteger
        return id is UInteger && id.toInt() < 26000
    }

    @ExperimentalCoroutinesApi
    private fun CoroutineScope.monitorByCallback(node: UaNode, item: DataItem): Job {
        val flow: Flow<DataValue> = callbackFlow {
            val observer = AttributeObserver { _, attributeId, value ->
                if (item.isSamplingEnabled && attributeId.uid() == item.readValueId.attributeId) {
                    // TODO these values need indexRange, timestamps, dataEncoding, etc... applied
                    sendBlocking(value as DataValue)
                }
            }

            send(
                node.readAttribute(
                    AttributeContext(server),
                    item.readValueId.attributeId,
                    TimestampsToReturn.Both,
                    item.readValueId.indexRange,
                    item.readValueId.dataEncoding
                )
            )

            node.addAttributeObserver(observer)
            awaitClose { node.removeAttributeObserver(observer) }
        }

        return launch {
            flow.conflate()
                .onEach {
                    item.setValue(it)
                    delay(item.samplingInterval.toLong())
                }
                .collect()
        }
    }

    private fun CoroutineScope.monitorBySampling(node: UaNode, item: DataItem): Job {
        val flow: Flow<DataValue> = flow {
            while (true) {
                if (item.isSamplingEnabled) {
                    val value: DataValue = node.readAttribute(
                        AttributeContext(server),
                        item.readValueId.attributeId,
                        TimestampsToReturn.Both,
                        item.readValueId.indexRange,
                        item.readValueId.dataEncoding
                    )

                    emit(value)
                }

                delay(item.samplingInterval.toLong())
            }
        }

        return launch { flow.onEach { item.setValue(it) }.collect() }
    }

    /**
     * Invoke one or more methods belonging to this [MethodServices].
     *
     * @param context  the [CallContext].
     * @param requests The [CallMethodRequest]s for the methods to invoke.
     */
    override fun call(context: CallContext, requests: List<CallMethodRequest>) {
        val results = Lists.newArrayListWithCapacity<CallMethodResult>(requests.size)

        for (request in requests) {
            val handler = getInvocationHandler(
                request.objectId,
                request.methodId
            ).orElse(MethodInvocationHandler.NODE_ID_UNKNOWN)

            try {
                results.add(handler.invoke(context, request))
            } catch (t: Throwable) {
                LoggerFactory.getLogger(javaClass)
                    .error("Uncaught Throwable invoking method handler for methodId={}.", request.methodId, t)

                results.add(
                    CallMethodResult(
                        StatusCode(StatusCodes.Bad_InternalError),
                        arrayOfNulls(0), arrayOfNulls(0), arrayOfNulls(0)
                    )
                )
            }

        }

        context.success(results)
    }

    /**
     * Get the [MethodInvocationHandler] for the method identified by `methodId`, if it exists.
     *
     * @param objectId the [NodeId] identifying the object the method will be invoked on.
     * @param methodId the [NodeId] identifying the method.
     * @return the [MethodInvocationHandler] for `methodId`, if it exists.
     */
    private fun getInvocationHandler(objectId: NodeId, methodId: NodeId): Optional<MethodInvocationHandler> {
        return nodeManager.getNode(objectId).flatMap { node ->
            var methodNode: UaMethodNode? = null

            if (node is UaObjectNode) {
                methodNode = node.findMethodNode(methodId)
            } else if (node is UaObjectTypeNode) {
                methodNode = node.findMethodNode(methodId)
            }

            if (methodNode != null) {
                Optional.of(methodNode.invocationHandler)
            } else {
                Optional.empty()
            }
        }
    }

}

fun DemoNamespace.addFolderNode(parentNodeId: NodeId, name: String): UaFolderNode {
    val folderNode = UaFolderNode(
        nodeContext,
        parentNodeId.resolve(name),
        QualifiedName(namespaceIndex, name),
        LocalizedText(name)
    )

    nodeManager.addNode(folderNode)

    folderNode.inverseReferenceTo(
        parentNodeId,
        Identifiers.HasComponent
    )

    return folderNode
}

fun DemoNamespace.addVariableNode(
    parentNodeId: NodeId,
    name: String,
    nodeId: NodeId = parentNodeId.resolve(name),
    dataType: BuiltinDataType = BuiltinDataType.Int32
): UaVariableNode {

    return addVariableNode(
        parentNodeId,
        name,
        nodeId,
        dataType.nodeId,
        dataType.defaultValue()
    )
}

fun DemoNamespace.addVariableNode(
    parentNodeId: NodeId,
    name: String,
    nodeId: NodeId = parentNodeId.resolve(name),
    dataTypeId: NodeId,
    value: Any
): UaVariableNode {

    val variableNode = UaVariableNode.UaVariableNodeBuilder(nodeContext).run {
        setNodeId(nodeId)
        setAccessLevel(AccessLevel.toValue(AccessLevel.READ_WRITE))
        setUserAccessLevel(AccessLevel.toValue(AccessLevel.READ_WRITE))
        setBrowseName(QualifiedName(namespaceIndex, name))
        setDisplayName(LocalizedText.english(name))
        setDataType(dataTypeId)
        setTypeDefinition(Identifiers.BaseDataVariableType)
        setMinimumSamplingInterval(100.0)
        setValue(DataValue(Variant(value)))

        build()
    }

    variableNode.filterChain.addFirst(AttributeLoggingFilter())

    nodeManager.addNode(variableNode)

    variableNode.inverseReferenceTo(
        parentNodeId,
        Identifiers.HasComponent
    )

    return variableNode
}

class AttributeLoggingFilter @JvmOverloads constructor(
    private val predicate: (AttributeId) -> Boolean = { true }
) : AttributeFilter {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun getAttribute(
        ctx: GetAttributeContext,
        attributeId: AttributeId
    ): Any? {

        val value = ctx.getAttribute(attributeId)

        // only log external reads
        if (predicate(attributeId) && ctx.session.isPresent) {
            logger.debug(
                "get nodeId={} attributeId={} value={}",
                ctx.node.nodeId, attributeId, value
            )
        }

        return value
    }

    override fun setAttribute(
        ctx: SetAttributeContext,
        attributeId: AttributeId,
        value: Any?
    ) {

        // only log external writes
        if (predicate(attributeId) && ctx.session.isPresent) {
            logger.debug(
                "set nodeId={} attributeId={} value={}",
                ctx.node.nodeId, attributeId, value
            )
        }

        ctx.setAttribute(attributeId, value)
    }

}
