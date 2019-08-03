package com.wanshifu.transformers.common.remote.client

import com.wanshifu.transformers.common.remote.server.ServerProvider
import spock.lang.Specification

class RpcServerTest extends Specification {

    def "test"() {

        ServerProvider provider = new ServerProvider("localhost", 9876)

        when:
        provider.registerService(TestService, new TestServiceImpl())

        provider.start()

        synchronized (this) {
            this.wait()
        }

        then:
        1

    }

}
