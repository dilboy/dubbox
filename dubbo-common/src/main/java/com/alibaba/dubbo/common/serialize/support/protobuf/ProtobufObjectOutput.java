package com.alibaba.dubbo.common.serialize.support.protobuf;

import java.io.IOException;
import java.io.OutputStream;

import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.google.protobuf.MessageLite;

public class ProtobufObjectOutput implements ObjectOutput
{
    OutputStream protobufOutput;

    public ProtobufObjectOutput(OutputStream os)
    {
        this.protobufOutput = os;
//        mH2o = new Hessian2Output(os);
//        mH2o.setSerializerFactory(Hessian2SerializerFactory.SERIALIZER_FACTORY);
    }

   

    public void writeObject(Object obj) throws IOException
    {
        if(obj instanceof MessageLite){
            ((MessageLite) obj).writeTo(protobufOutput);
        }else{
            throw new RuntimeException("protobuf unsupported type:"+obj.getClass().getName());
        }
        
    }

    public void flushBuffer() throws IOException
    {
        protobufOutput.flush();
    }



    @Override
    public void writeBool(boolean v) throws IOException {
        System.out.println("protobuf不支持writeBool值");
    }



    @Override
    public void writeByte(byte v) throws IOException {
        System.out.println("protobuf不支持writeByte值");
    }



    @Override
    public void writeShort(short v) throws IOException {
        System.out.println("protobuf不支持short值");
    }



    @Override
    public void writeInt(int v) throws IOException {
        System.out.println("protobuf不支持write int值");
    }



    @Override
    public void writeLong(long v) throws IOException {
        System.out.println("protobuf不支持write long值");
    }



    @Override
    public void writeFloat(float v) throws IOException {
        System.out.println("protobuf不支持write float值");
    }



    @Override
    public void writeDouble(double v) throws IOException {
        System.out.println("protobuf不支持write double值");
    }



    @Override
    public void writeUTF(String v) throws IOException {
        
        System.out.println("protobuf不支持write utf值:"+v);
    }



    @Override
    public void writeBytes(byte[] v) throws IOException {
        System.out.println("protobuf不支持write bytes值");
        
    }



    @Override
    public void writeBytes(byte[] v, int off, int len) throws IOException {
        System.out.println("protobuf不支持write bytes值");
    }
}
