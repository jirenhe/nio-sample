import io.netty.handler.codec.serialization.ClassResolver

class MapClassResolver implements ClassResolver{
    @Override
    Class<?> resolve(String className) throws ClassNotFoundException {
        return MapClassResolver.class.classLoader.loadClass(className)
    }
}
