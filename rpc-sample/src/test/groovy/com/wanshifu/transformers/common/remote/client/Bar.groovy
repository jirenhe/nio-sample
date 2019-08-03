package com.wanshifu.transformers.common.remote.client

class Bar {

    private int f1

    private Integer f2

    private Double f3

    private BigDecimal f4

    private Date date

    private Fsub fsub

    @Override
    public String toString() {
        return "Bar{" +
                "f1=" + f1 +
                ", f2=" + f2 +
                ", f3=" + f3 +
                ", f4=" + f4 +
                ", date=" + date +
                ", fsub=" + fsub +
                '}';
    }

    static class Fsub {

        private String ff1

        private Long ff2

        String getFf1() {
            return ff1
        }

        void setFf1(String ff1) {
            this.ff1 = ff1
        }

        Long getFf2() {
            return ff2
        }

        void setFf2(Long ff2) {
            this.ff2 = ff2
        }


        @Override
        public String toString() {
            return "Fsub{" +
                    "ff1='" + ff1 + '\'' +
                    ", ff2=" + ff2 +
                    '}';
        }
    }

    int getF1() {
        return f1
    }

    void setF1(int f1) {
        this.f1 = f1
    }

    Integer getF2() {
        return f2
    }

    void setF2(Integer f2) {
        this.f2 = f2
    }

    Double getF3() {
        return f3
    }

    void setF3(Double f3) {
        this.f3 = f3
    }

    BigDecimal getF4() {
        return f4
    }

    void setF4(BigDecimal f4) {
        this.f4 = f4
    }

    Date getDate() {
        return date
    }

    void setDate(Date date) {
        this.date = date
    }

    Fsub getFsub() {
        return fsub
    }

    void setFsub(Fsub fsub) {
        this.fsub = fsub
    }


}
