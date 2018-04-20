package com.teradata.mail;

import javax.mail.*;
import javax.mail.internet.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import javax.mail.Message.RecipientType;

public class SendMail {
    /*
    /**
     * Send mail with specified params
     * @param from who this is from
     * @param to who this is for
     * @param subject subject of the mail
     * @param text body of the mail
     */
    /*String bodymsg ;
    SendMail(String body){
        this.bodymsg=body;
    }*/
    public  void send(final Properties props,String bodymsg){

        String from = props.getProperty("mail.from");
        String to = props.getProperty("mail.to");
        String subject = props.getProperty("mail.subject");
        String body = bodymsg;

        Session mailSession = Session.getDefaultInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(props.getProperty("mail.user"), props.getProperty("mail.password"));
                    }
                });
        Message simpleMessage = new MimeMessage(mailSession);

        InternetAddress fromAddress = null;
        InternetAddress toAddress = null;
        try {
            fromAddress = new InternetAddress(from);
            toAddress = new InternetAddress(to);
        } catch (AddressException e) {
            e.printStackTrace();
        }

        try {
            simpleMessage.setFrom(fromAddress);
            simpleMessage.setRecipient(RecipientType.TO, toAddress);
            simpleMessage.setSubject(subject);
            simpleMessage.setText(body);

            Transport.send(simpleMessage);
        } catch (MessagingException e) {
            System.err.println("Error sending mail");
            e.printStackTrace();
        }
    }
    public Properties loadPropertiesFile(){

        final Properties props = new Properties();
        try {
            props.load(new FileInputStream("C:\\Users\\DL250031\\IdeaProjects\\Flink\\src\\Resources\\mail.properties"));
        } catch (FileNotFoundException e1) {
            System.err.println("Properties file not found");
            e1.printStackTrace();
        } catch (IOException e1) {
            System.err.println("Error loading properties file");
            e1.printStackTrace();
        }
        return props;
    }

   /* public static void main(String[] args) {


         SendMail mail = new SendMail();
         Properties props = mail.loadPropertiesFile();
        mail.send(props,"Alert Generated ..");
    }*/
}
