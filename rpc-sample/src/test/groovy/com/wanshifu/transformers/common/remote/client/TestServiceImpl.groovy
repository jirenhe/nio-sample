package com.wanshifu.transformers.common.remote.client

import java.util.concurrent.TimeUnit

class TestServiceImpl implements TestService {

    @Override
    Bar request(Foo foo) {
        println "receiver request : ${foo.toString()}"
        Bar bar = new Bar().with {
            it.f1 = 1
            it.f2 = 2
            it.f3 = 3.1
            it.f4 = new BigDecimal(1.23)
            it.date = new Date()
            it.fsub = new Bar.Fsub().with {
                it.ff1 = 'asda'
                it.ff2 = 1231L
                it
            }
            it
        }
        return bar
    }

    @Override
    Bar request2() {
        println "receiver request no para "
        Bar bar = new Bar().with {
            it.f1 = 1
            it.f2 = 2
            it.f3 = 3.1
            it.f4 = new BigDecimal(1.23)
            it.date = new Date()
            it.fsub = new Bar.Fsub().with {
                it.ff1 = 'asda'
                it.ff2 = 1231L
                it
            }
            it
        }
        return bar
    }

    @Override
    void request3(Foo foo) {
        println "receiver request : ${foo.toString()}"
    }

    @Override
    void testFail() {
        throw new RuntimeException("test fail!")
    }

    @Override
    void testTimeOut(long time) {
        TimeUnit.MILLISECONDS.sleep(time)
    }
}
