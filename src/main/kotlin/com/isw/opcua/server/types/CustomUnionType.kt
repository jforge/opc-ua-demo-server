/*
 * Copyright (c) 2020 the Eclipse Milo Authors
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package com.isw.opcua.server.types

import com.isw.opcua.server.namespaces.demo.DemoNamespace
import org.eclipse.milo.opcua.stack.core.StatusCodes
import org.eclipse.milo.opcua.stack.core.UaSerializationException
import org.eclipse.milo.opcua.stack.core.serialization.SerializationContext
import org.eclipse.milo.opcua.stack.core.serialization.UaDecoder
import org.eclipse.milo.opcua.stack.core.serialization.UaEncoder
import org.eclipse.milo.opcua.stack.core.serialization.UaStructure
import org.eclipse.milo.opcua.stack.core.serialization.codecs.GenericDataTypeCodec
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned
import org.eclipse.milo.opcua.stack.core.types.structured.Union

class CustomUnionType private constructor(private val type: Type, private val value: Any?) : Union(), UaStructure {

    override fun getTypeId(): ExpandedNodeId {
        return TYPE_ID
    }

    override fun getBinaryEncodingId(): ExpandedNodeId {
        return BINARY_ENCODING_ID
    }

    override fun getXmlEncodingId(): ExpandedNodeId {
        // XML encoding not supported
        return ExpandedNodeId.NULL_VALUE
    }

    fun asFoo(): UInteger? {
        return value as UInteger?
    }

    fun asBar(): String? {
        return value as String?
    }

    val isNull: Boolean
        get() = type == Type.Null

    val isFoo: Boolean
        get() = type == Type.Foo

    val isBar: Boolean
        get() = type == Type.Bar

    internal enum class Type {
        Null, Foo, Bar
    }

    class Codec : GenericDataTypeCodec<CustomUnionType>() {

        override fun getType(): Class<CustomUnionType> {
            return CustomUnionType::class.java
        }

        override fun decode(context: SerializationContext, decoder: UaDecoder): CustomUnionType {
            val switchValue = decoder.readUInt32("SwitchValue")

            return when (switchValue.toInt()) {
                0 -> ofNull()
                1 -> {
                    val foo = decoder.readUInt32("foo")
                    ofFoo(foo)
                }
                2 -> {
                    val bar = decoder.readString("bar")
                    ofBar(bar)
                }
                else -> throw UaSerializationException(
                    StatusCodes.Bad_DecodingError,
                    "unknown field in Union CustomUnionType: $switchValue"
                )
            }
        }

        override fun encode(
            context: SerializationContext,
            encoder: UaEncoder,
            value: CustomUnionType
        ) {

            encoder.writeUInt32(
                "SwitchValue",
                Unsigned.uint(value.type.ordinal)
            )

            when (value.type) {
                Type.Null -> {
                    // noop
                }
                Type.Foo -> {
                    encoder.writeUInt32("foo", value.asFoo())
                }
                Type.Bar -> {
                    encoder.writeString("bar", value.asBar())
                }
            }
        }

    }

    companion object {
        val TYPE_ID: ExpandedNodeId = ExpandedNodeId.parse(
            String.format(
                "nsu=%s;s=%s",
                DemoNamespace.NAMESPACE_URI,
                "DataType.CustomUnionType"
            )
        )

        val BINARY_ENCODING_ID: ExpandedNodeId = ExpandedNodeId.parse(
            String.format(
                "nsu=%s;s=%s",
                DemoNamespace.NAMESPACE_URI,
                "DataType.CustomUnionType.BinaryEncoding"
            )
        )

        fun ofNull(): CustomUnionType {
            return CustomUnionType(Type.Null, null)
        }

        fun ofFoo(value: UInteger?): CustomUnionType {
            return CustomUnionType(Type.Foo, value)
        }

        fun ofBar(value: String?): CustomUnionType {
            return CustomUnionType(Type.Bar, value)
        }
    }

}