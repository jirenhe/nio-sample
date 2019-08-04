package com.wanshifu.transformers.common.remote.client


import spock.lang.Specification

import java.lang.reflect.UndeclaredThrowableException

class RpcClientTest extends Specification {

    def "test"() {

        setup:
        Foo foo = getFoo()
        TestService service = RpcServiceFactory.getKeepAliveRemoteService(TestService, "localhost", 9876)

        expect:
        println(service.request(a))

        where:
        a << [foo, foo]

    }

    def "testRequest1"() {

        setup:
        Foo foo = getFoo()
        TestService service = RpcServiceFactory.getShortConnectRemoteService(TestService, "localhost", 9876)

        expect:
        println(service.request(a))


        where:
        a << [foo, foo]

    }

    def "testRequest2"() {

        setup:
        TestService service = RpcServiceFactory.getKeepAliveRemoteService(TestService, "localhost", 9876)

        expect:
        println(service.request2())

        where:
        a << [null, null]

    }

    def "testRequest3"() {

        setup:
        Foo foo = getFoo()
        TestService service = RpcServiceFactory.getKeepAliveRemoteService(TestService, "localhost", 9876)

        expect:
        println(service.request3(a))

        where:
        a << [foo, foo]

    }

    def "testRemoteFail"() {

        setup:
        TestService service = RpcServiceFactory.getKeepAliveRemoteService(TestService, "localhost", 9876)

        when:
        service.testFail()

        then:
        thrown(UndeclaredThrowableException)

        where:
        a << [1, 1]

    }

    def "testTimeOut"() {

        setup:
        TestService service = RpcServiceFactory.getShortConnectRemoteService(TestService, "localhost", 9876, 10)

        when:
        service.testTimeOut(100)

        then:
        thrown(UndeclaredThrowableException)

        where:
        a << [1, 1]

    }

    def "testNoRegisterService"() {

        setup:
        FooService service = RpcServiceFactory.getKeepAliveRemoteService(FooService, "localhost", 9876)

        when:
        service.test()

        then:
        thrown(UndeclaredThrowableException)

        where:
        a << [1, 1]

    }

    Foo getFoo() {
        return new Foo().with {
            it.f1 = 123
            it.f2 = 12412
            it.f3 = 1245.12
            it.f4 = new BigDecimal(5456.32)
            it.date = new Date()
            it.fsub = new Foo.Fsub().with {
                it.ff1 = "asdq"
                it.ff2 = 12312L
                it
            }
            it
        }
    }
}
