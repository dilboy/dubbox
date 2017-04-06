package com.alibaba.dubbo.common.serialize.support.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.serialize.Serialization;

public class ProtobufSerialization implements  Serialization{

    @Override
    public byte getContentTypeId() {
        return 11;
    }

    @Override
    public String getContentType() {
        return "x-application/protobuf";
    }

    @Override
    public ObjectOutput serialize(URL url, OutputStream output) throws IOException {
        return new ProtobufObjectOutput(output);
    }

    @Override
    public ObjectInput deserialize(URL url, InputStream input) throws IOException {
        return new ProtobufObjectInput(input);
    }

}
