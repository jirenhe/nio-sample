package com.wanshifu.transformers.common.remote.client

interface TestService {

    Bar request(Foo foo)

    Bar request2()

    void request3(Foo foo)

    void testFail()

}