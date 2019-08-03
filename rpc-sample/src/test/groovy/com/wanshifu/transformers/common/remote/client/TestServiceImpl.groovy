package com.wanshifu.transformers.common.remote.client

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
}
