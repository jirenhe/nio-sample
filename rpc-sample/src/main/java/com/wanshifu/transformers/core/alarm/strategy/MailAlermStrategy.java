package com.wanshifu.transformers.core.alarm.strategy;

import com.sun.mail.util.MailSSLSocketFactory;
import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.core.alarm.IAlarm;
import com.wanshifu.transformers.core.alarm.configuration.MailConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author ：Smile(wangyajun)
 * @date ：Created in 2019/5/29 11:28
 * @description： 邮件告警
 */
public class MailAlermStrategy extends Configurable implements IAlarm {

    private List<String> receivers;

    private static volatile boolean isInit = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MailAlermStrategy.class);

    public static final String REGEX_EMAIL = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";

    private Session session;


    @Override
    public void init() throws InitException {
        Properties prop = new Properties();
        //协议
        prop.setProperty("mail.transport.protocol", MailConfig.MAIL_TRANSPORT_PROTOCOL);
        //服务器
        prop.setProperty("mail.smtp.host", MailConfig.MAIL_SENDER_SMTP_HOST);
        //端口
        prop.setProperty("mail.smtp.port", ""+MailConfig.MAIL_SENDER_SMTP_PORT);
        //使用smtp身份验证
        prop.setProperty("mail.smtp.auth", "true");
        //使用SSL，企业邮箱必需！
        //开启安全协议
        MailSSLSocketFactory sf = null;
        try {
            sf = new MailSSLSocketFactory();
            sf.setTrustAllHosts(true);
        } catch (GeneralSecurityException e1) {
            e1.printStackTrace();
        }
        prop.put("mail.smtp.ssl.enable", "true");
        prop.put("mail.smtp.ssl.socketFactory", sf);
        //
        //获取Session对象
        session = Session.getDefaultInstance(prop, new Authenticator() {
            //此访求返回用户和密码的对象
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(MailConfig.MAIL_SENDER_USERNAME, MailConfig.MAIL_SENDER_PASSWORD);
            }
        });

        //初始化邮件接收方
        receivers = new ArrayList<>();
        String alarmUsers = getConfiguration().getNecessaryValue(MailConfig.ALARM_USERS);

        //邮箱正则表达式校验
        if (!check(alarmUsers)){
            throw new IllegalArgumentException(String.format("配置的%s邮件地址错误", MailConfig.ALARM_USERS));
        }


        for (String s : alarmUsers.split(",")) {
            receivers.add(s);
        }
        isInit = true;
    }

    /**
     * 根据配置文件中的邮件字符串校验邮箱正则
     * @param alarmUsers
     * @return
     */
    private boolean check(String alarmUsers) {
        for (String s : alarmUsers.split(",")) {
            if (!Pattern.matches(REGEX_EMAIL, s)){
                return false;
            }
        }
        return true;
    }

    @Override
    public void send(String content) {
        if (isInit) {
            sendEmail(content);
        }
    }

    private void sendEmail(String content) {
        //设置session的调试模式，发布时取消
//        s.setDebug(true);
        MimeMessage mimeMessage = new MimeMessage(session);
        try {
            mimeMessage.setFrom(new InternetAddress(MailConfig.MAIL_SENDER_USERNAME, MailConfig.MAIL_SENDER_USERNAME));

            for (String receiver : receivers) {
                mimeMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(receiver));
            }
            //设置主题
            mimeMessage.setSubject("transform告警通知");
            mimeMessage.setSentDate(new Date());
            //设置内容
            mimeMessage.setText(content);
            mimeMessage.saveChanges();
            //发送
            Transport.send(mimeMessage);
        } catch (MessagingException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


}
