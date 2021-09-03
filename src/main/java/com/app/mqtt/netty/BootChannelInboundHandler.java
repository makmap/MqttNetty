package com.app.mqtt.netty;




import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * MQTT服务端I/O数据读写处理类
 *
 * 一个MQTT数据包由：固定头（Fixed header）、可变头（Variable header）、消息体（payload）三部分构成。
 * 如固定头：mqttMessage.fixedHeader()
 *
 * MQTT 的 Topic 有层级结构，并且支持通配符 + 和 #:
 *
 * +是匹配单层的通配符。比如 news/+ 可以匹配 news/sports，news/+/basketball 可匹配到 news/sports/basketball
 * # 是一到多层的通配符。比如 news/# 可以匹配 news、 news/sports、news/sports/basketball 以及 news/sports/basketball/x 等等
 *
 */
@Slf4j
@ChannelHandler.Sharable
public class BootChannelInboundHandler extends ChannelInboundHandlerAdapter {

    /**
     * 	客户端与服务端第一次建立连接时执行 在channelActive方法之前执行
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        log.warn("channelRegistered : {}" , ctx);
    }

    /**
     * 	客户端与服务端 断连时执行 channelInactive方法之后执行
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        log.warn("channelUnregistered : {}" , ctx);
    }

    /**
     * 	从客户端收到新的数据时，这个方法会在收到消息时被调用
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (null != msg) {
            MqttMessage mqttMessage = (MqttMessage) msg;
            log.info("channelRead->MqttMessage : {}", mqttMessage.toString());
            MqttFixedHeader mqttFixedHeader = mqttMessage.fixedHeader();
            Channel channel = ctx.channel();

            if(mqttFixedHeader.messageType().equals(MqttMessageType.CONNECT)){
                //	在一个网络连接上，客户端只能发送一次CONNECT报文。服务端必须将客户端发送的第二个CONNECT报文当作协议违规处理并断开客户端的连接
                //	to do 建议connect消息单独处理，用来对客户端进行认证管理等 这里直接返回一个CONNACK消息
                BootMqttMsgBack.connAck(channel, mqttMessage);
            }

            switch (mqttFixedHeader.messageType()){
                case PUBLISH:		//	客户端发布消息
                    //	PUBACK报文是对QoS 1等级的PUBLISH报文的响应
//                    log.info("123");
                    BootMqttMsgBack.publish(channel, mqttMessage);
                    break;
                case PUBREL:		//	发布释放
                    //	PUBREL报文是对PUBREC报文的响应
                    //	to do
                    BootMqttMsgBack.pubComp(channel, mqttMessage);
                    break;
                case SUBSCRIBE:		//	客户端订阅主题
                    //	客户端向服务端发送SUBSCRIBE报文用于创建一个或多个订阅，每个订阅注册客户端关心的一个或多个主题。
                    //	为了将应用消息转发给与那些订阅匹配的主题，服务端发送PUBLISH报文给客户端。
                    //	SUBSCRIBE报文也（为每个订阅）指定了最大的QoS等级，服务端根据这个发送应用消息给客户端
                    // 	to do
                    BootMqttMsgBack.subAsk(channel, mqttMessage);
                    break;
                case UNSUBSCRIBE:	//	客户端取消订阅
                    //	客户端发送UNSUBSCRIBE报文给服务端，用于取消订阅主题
                    //	to do
                    BootMqttMsgBack.unSubAck(channel, mqttMessage);
                    break;
                case PINGREQ:		//	客户端发起心跳
                    //	客户端发送PINGREQ报文给服务端的
                    //	在没有任何其它控制报文从客户端发给服务的时，告知服务端客户端还活着
                    //	请求服务端发送 响应确认它还活着，使用网络以确认网络连接没有断开
                    BootMqttMsgBack.pingResp(channel, mqttMessage);
                    break;
                case DISCONNECT:	//	客户端主动断开连接
                    //	DISCONNECT报文是客户端发给服务端的最后一个控制报文， 服务端必须验证所有的保留位都被设置为0
                    //	to do
                    break;
                default:
                    break;
            }
        } else {
            log.info("channelRead : {}", msg);
        }
    }

    /**
     * 	从客户端收到新的数据、读取完成时调用
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
//        log.warn("channelReadComplete : {}" , ctx);
    }

    /**
     * 	当出现 Throwable 对象才会被调用，即当 Netty 由于 IO 错误或者处理器在处理事件时抛出的异常时
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
        log.warn("exceptionCaught : {}" , ctx);
    }

    /**
     * 	客户端与服务端第一次建立连接时执行
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.warn("channelActive : {}" , ctx);
    }

    /**
     * 	客户端与服务端 断连时执行
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception, IOException {
        super.channelInactive(ctx);
        log.warn("channelInactive : {}" , ctx);
    }

    /**
     * 	服务端 当读超时时 会调用这个方法
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception, IOException {
        super.userEventTriggered(ctx, evt);
        ctx.close();
        log.warn("userEventTriggered : {}" , ctx);
    }


    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        log.warn("channelWritabilityChanged : {}" , ctx);
    }

}
