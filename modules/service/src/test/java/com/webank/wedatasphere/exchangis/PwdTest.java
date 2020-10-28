package com.webank.wedatasphere.exchangis;


import com.webank.wedatasphere.exchangis.common.util.CryptoUtils;
import org.junit.Test;


public class PwdTest {

    @Test
    public void  test() {

       String res =  CryptoUtils.md5("vTJvoRvR2fzx@20", "admin", 2);
       System.out.println(res);
    }

}
