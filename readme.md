# 阶段目标
- [x] namenode启动 
- [x] namenode启动连接其它的namdenode
- [x] namenode id大的连小的，如果小的还没启动，需要重试
- [x] string作为协议
- [x] string作为协议肯定是不行的，需要改合适的协议



# 问题
-  An exceptionCaught() event was fired, and it reached at the tail of the pipeline. It usually means the last handler in the pipeline did not handle the exception
 
 **解决：** 
需要重写handler里的exceptionCaught()



- 
> io.netty.channel.ChannelPipelineException: rechard.learn.namenode.network.NettyChannelHandler is not a @Sharable handler, so can't be added or removed multiple times.

**原因**

```
//如下注释每次都是用的同个handler对象，所以就报错了，改成Supplier
public  class BaseChannelInitializer extends ChannelInitializer {

    //private java.util.List<ChannelHandler> handlerSuppliers = new ArrayList<>();

    private java.util.List<Supplier<ChannelHandler>> handlerSuppliers = new ArrayList<>();

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        
        //for (ChannelHandler handler : handlerSuppliers) {
        //    pipeline.addLast(handler);
        //}        

        for (Supplier<ChannelHandler> handlerSupplier : handlerSuppliers) {
            pipeline.addLast(handlerSupplier.get());
        }
    }
    public void addHandler(Supplier<ChannelHandler> supplier) {
        handlerSuppliers.add(supplier);
    }

//    public void addHandler(ChannelHandler handler) {
//        this.handlers.add(handler);
//    }
}
```
**解决**
代码里未注释部分

- java.lang.UnsupportedOperationException: direct buffer
直接堆内存不支持转成byte[]操作

# protobuf

```
mvn protobuf:compile && mvn install
```

# Netty
## Netty的handler的顺序会对消息的解析产生重要的影响

 ![env](doc/error_config_pipeline.png)
红色表示outbound,我将StringEncode和LengthFieldPrepender配错了位置
导致server端解析出错



