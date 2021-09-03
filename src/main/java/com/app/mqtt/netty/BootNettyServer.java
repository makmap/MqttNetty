package com.app.mqtt.netty;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;


/**
 * Springboot+Netty搭建基于MQTT协议的服务端
 * https://blog.csdn.net/myyhtw/article/details/114041042
 */
@Slf4j
public class BootNettyServer {

    private int port = 1883;

    private NioEventLoopGroup bossGroup;

    private NioEventLoopGroup workGroup;

    /**
     * 	启动服务
     * @throws InterruptedException
     */
    public void startup() {

        try {
            bossGroup = new NioEventLoopGroup(1);
            workGroup = new NioEventLoopGroup();

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workGroup);
            bootstrap.channel(NioServerSocketChannel.class);

            bootstrap.option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_RCVBUF, 10485760);

            bootstrap.childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline channelPipeline = ch.pipeline();
                    // 设置读写空闲超时时间
                    channelPipeline.addLast(new IdleStateHandler(600, 600, 1200));
                    channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    channelPipeline.addLast("decoder", new MqttDecoder());
                    channelPipeline.addLast(new BootChannelInboundHandler());
                }
            });
            ChannelFuture f = bootstrap.bind(port).sync();
            f.channel().closeFuture().sync();

        } catch (Exception e) {
            System.out.println("start exception"+e.toString());
        }

    }

    /**
     * 	关闭服务
     */
    public void shutdown() throws InterruptedException {
        if (workGroup != null && bossGroup != null) {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
            System.out.println("shutdown success");
        }
    }

}
