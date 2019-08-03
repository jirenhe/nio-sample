package com.wanshifu.transformers.common.remote.client

import org.junit.Test
import spock.lang.Specification

class RpcClientTest extends Specification {

    def "test"() {

        setup:
        Foo foo = getFoo()
        TestService service = RpcServiceFactory.getLongKeepRemoteService(TestService, "localhost", 9876)

        expect:
        println(service.request(a))
        1

        where:
        a << [foo, foo]

    }

    def "test2"() {

        setup:
        Foo foo = getFoo()

        expect:
        RpcServiceFactory.getOnceRemoteService(TestService, "localhost", 9876)request(a)

        where:
        a << [foo, foo]

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
