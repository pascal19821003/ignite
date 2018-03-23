/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.test;

//import java.sql.*;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * This example demonstrates usage of Ignite JDBC driver.
 * <p>
 * Ignite nodes must be started in separate process using {@link ExampleNodeStartup} before running this example.
 */
public class JdbcExample {

    public static AtomicLong atomicLong = new AtomicLong(1);
    public static int TOTAL_RECORD = 100000;
    public static int CONNECTOR_NUM = 20;

    public static Date firstStartTime = null;
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
      new JdbcExample().start();
    }

    public void start() throws Exception{
        JdbcExample.executeDelete();
        firstStartTime = new Date();
        long start = System.currentTimeMillis();
        System.out.printf("第一次开始时间： %s\n", firstStartTime);
        Thread[] threads = new Thread[CONNECTOR_NUM];
        for(int i=0;i<CONNECTOR_NUM;i++){
            threads [i] = new Thread(new SmallWorker(i));
            threads [i].start();
        }
        for(int i=0;i<CONNECTOR_NUM;i++){
            threads [i].join();
        }
        System.out.println("aaaa   " + (System.currentTimeMillis() - start));
    }

    class Worker implements Runnable{
        int code;
        public Worker(int code){
            this.code  = code;
        }
        @Override
        public void run() {
            try {
                executeInsert(code);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public void executeInsert(int code) throws Exception{
            print("JDBC example started.");

            // Open JDBC connection
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://centosb/")) {
                print("Connected to server.");

                Date startTime = new Date();
                System.out.printf("code: %d , 开始插入：%s \n" , code, startTime);
                try(PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO TRAN " +
                                "(ID, CREATE_TM, SETTLE_DT, SYS_TRA_NO, TRANS_ID, " +
                                "SETTLE_IN, TRANS_AT, MSG_TP, LOC_TRANS_DT_TM, MCHNT_TP, " +
                                "RETRI_REF_NO, PRI_ACCT_NO, BIN, CARD_ATTR, CARD_MEDIA, " +
                                "TERM_ID, MCHNT_CD, CARD_ACCPTR_NM_LOC, ACQ_INS_ID_CD, FWD_INS_ID_CD, " +
                                "RCV_INS_ID_CD, ISS_INS_ID_CD, ORIG_SYS_TRA_NO, ORIG_TRANSMSN_DT_TM, RSN_CD, " +
                                "AUTH_DT, AUTH_ID_RESP_CD, POS_ENTRY_MD_CD, POS_COND_CD, PROC_CD, " +
                                "SUBINST_CODE, INVALID) " +
                                "VALUES(?, ?, ?, ?, ?, " +
                                "?, ?, ?, ?, ?, " +
                                "?, ?, ?, ?, ?, " +
                                "?, ?, ?, ?, ?, " +
                                "?, ?, ?, ?, ?, " +
                                "?, ?, ?, ?, ?, " +
                                "?, ?)" )    ){

                    for(int i=0;i<TOTAL_RECORD/CONNECTOR_NUM;i++){
                        //2106|20160831134258|20160831|592014|00|S30|
                        // 2106|20160831134258|20160831|592014|S30|
                        /**
                         * ID	流水	ID	N	22
                         CREATE_TM	C	18	yyyyMMddHHmmssSSS
                         SETTLE_DT	清算日期	C	8	YYYYMMDD
                         SYS_TRA_NO	系统跟踪号	C	6	√
                         TRANS_ID	交易代码	C	3	√
                         */
                        stmt.setLong(1, atomicLong.getAndIncrement()); //ID
                        stmt.setTimestamp(2,new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)) ); // CREATE_TM
                        stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));//todo //, SETTLE_DT,
                        stmt.setString(4, "592014"); //SYS_TRA_NO,
                        stmt.setString(5, "S30");//TRANS_ID

                        // 1|32380|0220|0831134258|4899|
                        /**
                         * SETTLE_IN	清算标志	C	1	√
                         TRANS_AT	交易金额	N	12	√
                         MSG_TP	消息类型	C	4	√
                         LOC_TRANS_DT_TM	C	10
                         MCHNT_TP	C	4	√
                         */
                        stmt.setLong(6, 1);//SETTLE_IN,
                        Integer at = 1000 + (int)(Math.random()* 10000);
                        stmt.setString(7, at.toString());//TRANS_AT,//todo
                        stmt.setString(8, "0220");//MSG_TP,
                        stmt.setString(9, "0831134258");//LOC_TRANS_DT_TM,
                        stmt.setString(10, "4899");//MCHNT_TP,

                        // 245500440078|6222023********0000|622202|01|4|
                        /**
                         * RETRI_REF_NO	检索参考号	C	12	√
                         PRI_ACCT_NO	主帐号	C	21	√
                         BIN	卡	bin(含长度)	C	14
                         CARD_ATTR	卡性质	C	2	√
                         CARD_MEDIA	卡介质	C	1	√
                         */
                        stmt.setString(11, "245500440078");//"RETRI_REF_NO,
                        Integer no = 100 + (int)(Math.random()*100);
                        stmt.setString(12, "6222023000000000000"+no.toString());//PRI_ACCT_NO, //todo
                        stmt.setString(13, "622202");//BIN,
                        stmt.setString(14, "01");// CARD_ATTR,
                        stmt.setString(15, "4");//CARD_MEDIA, " +

                        // 01080209|898110248990017|北京某某科技有限公司|00049996|00049992|
                        /**
                         * TERM_ID	C	8
                         MCHNT_CD	C	15
                         CARD_ACCPTR_NM_LOC	C	40	√
                         ACQ_INS_ID_CD	受理机构标识码	C	13	√
                         FWD_INS_ID_CD	转发机构标识码	C	13	√
                         */
                        stmt.setString(16, "01080209");//"TERM_ID,
                        stmt.setString(17, "898110248990017");//MCHNT_CD,
                        stmt.setString(18, "北京某某科技有限公司");//CARD_ACCPTR_NM_LOC,
                        stmt.setString(19, "00049996");//ACQ_INS_ID_CD,
                        stmt.setString(20, "00049992");//FWD_INS_ID_CD, " +

                        // 01025800|01020000|044103|1021124550|0000|
                        /**
                         * RCV_INS_ID_CD	接收机构标识码	C	13	√
                         ISS_INS_ID_CD	发卡机构标识码	C	13	√
                         ORIG_SYS_TRA_NO	原始系统跟踪号	C	6	√
                         ORIG_TRANSMSN_DT_TM		原始系统日期时	C	10	√
                         RSN_CD	原因码	C	4	√
                         */
                        stmt.setString(21, "01025800");//"RCV_INS_ID_CD,
                        stmt.setString(22, "01020000");//ISS_INS_ID_CD,
                        stmt.setString(23, "044103");//ORIG_SYS_TRA_NO,
                        stmt.setTimestamp(24, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));;// ORIG_TRANSMSN_DT_TM,
                        stmt.setString(25, "0000");//RSN_CD, " +

                        // /?|N|012|00|200000|
                        /**
                         * AUTH_DT		授权日期		C	4
                         AUTH_ID_RESP_CD	授权标识应答码	C	7	√
                         POS_ENTRY_MD_CD	服务点输入方式	C	3	√
                         POS_COND_CD	服务点条件代码	C	2	√
                         PROC_CD	交易处理码	C	6	√
                         */
                        stmt.setTimestamp(26, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));;//"AUTH_DT,
                        stmt.setString(27, "N");//AUTH_ID_RESP_CD,
                        stmt.setString(28, "012");//POS_ENTRY_MD_CD,
                        stmt.setString(29, "00");//POS_COND_CD,
                        stmt.setString(30, "200000");//PROC_CD, " +

                        // 000100|0
                        /**
                         * SUBINST_CODE	所属分支机构	C	6	√
                         INVALID	交易是否有效	C	1	√
                         */
                        stmt.setString(31, "000100"); //"SUBINST_CODE,
                        stmt.setString(32, "0"); // INVALID) " +

                        stmt.addBatch();
                        if(i%50==0){
                            stmt.executeBatch();
//                        stmt.executeUpdate();
                        }
                    }
                    stmt.executeBatch();

                }
                Date endTime = new Date();
                System.out.printf("code: %d , 结束插入：%s \n" , code, endTime);
                System.out.printf("code: %d , 耗时（s）: %d  \n", code, (endTime.getTime() - startTime.getTime())/1000 );
                System.out.printf("当前总的耗时（s）: %d  \n",  (endTime.getTime() - firstStartTime.getTime())/1000 );
            }

            print("JDBC example finished.");
        }

    }


    class SmallWorker implements Runnable{
        int code;
        public SmallWorker(int code){
            this.code  = code;
        }
        @Override
        public void run() {
            try {
                executeInsert(code);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public void executeInsert(int code) throws Exception{
            print("JDBC example started.");

            // Open JDBC connection
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://centosb/")) {
                print("Connected to server.");

                Date startTime = new Date();
                System.out.printf("code: %d , 开始插入：%s \n" , code, startTime);
                try(PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO simple " +
                                "(ID, CREATE_TM, SYS_TRA_NO) " +
                                "VALUES(?, ?, ?)" )    ){

                    for(int i=0;i<TOTAL_RECORD/CONNECTOR_NUM;i++){
                        //2106|20160831134258|20160831|592014|00|S30|
                        // 2106|20160831134258|20160831|592014|S30|
                        /**
                         * ID	流水	ID	N	22
                         CREATE_TM	C	18	yyyyMMddHHmmssSSS
                         SETTLE_DT	清算日期	C	8	YYYYMMDD
                         SYS_TRA_NO	系统跟踪号	C	6	√
                         TRANS_ID	交易代码	C	3	√
                         */
                        stmt.setLong(1, atomicLong.getAndIncrement()); //ID
                        stmt.setTimestamp(2,new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)) ); // CREATE_TM
                        stmt.setString(3, "592014");

                        stmt.addBatch();
                        if(i%50==0){
                            stmt.executeBatch();
//                        stmt.executeUpdate();
                        }
                    }
                    stmt.executeBatch();

                }
                Date endTime = new Date();
                System.out.printf("code: %d , 结束插入：%s \n" , code, endTime);
                System.out.printf("code: %d , 耗时（s）: %d  \n", code, (endTime.getTime() - startTime.getTime())/1000 );
                System.out.printf("当前总的耗时（s）: %d  \n",  (endTime.getTime() - firstStartTime.getTime())/1000 );
            }

            print("JDBC example finished.");
        }

    }

    public static void executeDelete() throws Exception{
        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://centosb/")) {
            print("Connected to server.");
           try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("delete from Simple");
            }
        }
    }

    public static void executeSql() throws Exception{
        print("JDBC example started.");

        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://centosb/")) {
            print("Connected to server.");

//            // Create database objects.
//            try (Statement stmt = conn.createStatement()) {
//                // Create reference City table based on REPLICATED template.
//                stmt.executeUpdate("CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) " +
//                    "WITH \"template=replicated\"");
//
//                // Create table based on PARTITIONED template with one backup.
//                stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, " +
//                    "PRIMARY KEY (id, city_id)) WITH \"backups=1, affinityKey=city_id\"");
//
//                // Create an index.
//                stmt.executeUpdate("CREATE INDEX on Person (city_id)");
//            }
//
//            print("Created database objects.");
//
//            // Populate City table with PreparedStatement.
//            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO city (id, name) VALUES (?, ?)")) {
//                stmt.setLong(1, 1L);
//                stmt.setString(2, "Forest Hill");
//                stmt.executeUpdate();
//
//                stmt.setLong(1, 2L);
//                stmt.setString(2, "Denver");
//                stmt.executeUpdate();
//
//                stmt.setLong(1, 3L);
//                stmt.setString(2, "St. Petersburg");
//                stmt.executeUpdate();
//            }

            // Populate Person table with PreparedStatement.
//            try (PreparedStatement stmt =
//                conn.prepareStatement("INSERT INTO person (id, name, city_id) values (?, ?, ?)")) {
//                stmt.setLong(1, 1L);
//                stmt.setString(2, "John Doe");
//                stmt.setLong(3, 3L);
//                stmt.executeUpdate();
//
//                stmt.setLong(1, 2L);
//                stmt.setString(2, "Jane Roe");
//                stmt.setLong(3, 2L);
//                stmt.executeUpdate();
//
//                stmt.setLong(1, 3L);
//                stmt.setString(2, "Mary Major");
//                stmt.setLong(3, 1L);
//                stmt.executeUpdate();
//
//                stmt.setLong(1, 4L);
//                stmt.setString(2, "Richard Miles");
//                stmt.setLong(3, 2L);
//                stmt.executeUpdate();
//            }

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("delete from TRAN");
            }


            Date startTime = new Date();
            System.out.println("开始插入：" + startTime);
            try(PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO TRAN " +
                            "(ID, CREATE_TM, SETTLE_DT, SYS_TRA_NO, TRANS_ID, " +
                            "SETTLE_IN, TRANS_AT, MSG_TP, LOC_TRANS_DT_TM, MCHNT_TP, " +
                            "RETRI_REF_NO, PRI_ACCT_NO, BIN, CARD_ATTR, CARD_MEDIA, " +
                            "TERM_ID, MCHNT_CD, CARD_ACCPTR_NM_LOC, ACQ_INS_ID_CD, FWD_INS_ID_CD, " +
                            "RCV_INS_ID_CD, ISS_INS_ID_CD, ORIG_SYS_TRA_NO, ORIG_TRANSMSN_DT_TM, RSN_CD, " +
                            "AUTH_DT, AUTH_ID_RESP_CD, POS_ENTRY_MD_CD, POS_COND_CD, PROC_CD, " +
                            "SUBINST_CODE, INVALID) " +
                            "VALUES(?, ?, ?, ?, ?, " +
                            "?, ?, ?, ?, ?, " +
                            "?, ?, ?, ?, ?, " +
                            "?, ?, ?, ?, ?, " +
                            "?, ?, ?, ?, ?, " +
                            "?, ?, ?, ?, ?, " +
                            "?, ?)" )    ){

                for(int i=0;i<100000;i++){
                    //2106|20160831134258|20160831|592014|00|S30|
                    // 2106|20160831134258|20160831|592014|S30|
                    /**
                     * ID	流水	ID	N	22
                     CREATE_TM	C	18	yyyyMMddHHmmssSSS
                     SETTLE_DT	清算日期	C	8	YYYYMMDD
                     SYS_TRA_NO	系统跟踪号	C	6	√
                     TRANS_ID	交易代码	C	3	√
                     */
                    stmt.setLong(1, i); //ID
                    stmt.setTimestamp(2,new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)) ); // CREATE_TM
                    stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));//todo //, SETTLE_DT,
                    stmt.setString(4, "592014"); //SYS_TRA_NO,
                    stmt.setString(5, "S30");//TRANS_ID

                    // 1|32380|0220|0831134258|4899|
                    /**
                     * SETTLE_IN	清算标志	C	1	√
                     TRANS_AT	交易金额	N	12	√
                     MSG_TP	消息类型	C	4	√
                     LOC_TRANS_DT_TM	C	10
                     MCHNT_TP	C	4	√
                     */
                    stmt.setLong(6, 1);//SETTLE_IN,
                    Integer at = 1000 + (int)(Math.random()* 10000);
                    stmt.setString(7, at.toString());//TRANS_AT,//todo
                    stmt.setString(8, "0220");//MSG_TP,
                    stmt.setString(9, "0831134258");//LOC_TRANS_DT_TM,
                    stmt.setString(10, "4899");//MCHNT_TP,

                    // 245500440078|6222023********0000|622202|01|4|
                    /**
                     * RETRI_REF_NO	检索参考号	C	12	√
                     PRI_ACCT_NO	主帐号	C	21	√
                     BIN	卡	bin(含长度)	C	14
                     CARD_ATTR	卡性质	C	2	√
                     CARD_MEDIA	卡介质	C	1	√
                     */
                    stmt.setString(11, "245500440078");//"RETRI_REF_NO,
                    Integer no = 100 + (int)(Math.random()*100);
                    stmt.setString(12, "6222023000000000000"+no.toString());//PRI_ACCT_NO, //todo
                    stmt.setString(13, "622202");//BIN,
                    stmt.setString(14, "01");// CARD_ATTR,
                    stmt.setString(15, "4");//CARD_MEDIA, " +

                    // 01080209|898110248990017|北京某某科技有限公司|00049996|00049992|
                    /**
                     * TERM_ID	C	8
                     MCHNT_CD	C	15
                     CARD_ACCPTR_NM_LOC	C	40	√
                     ACQ_INS_ID_CD	受理机构标识码	C	13	√
                     FWD_INS_ID_CD	转发机构标识码	C	13	√
                     */
                    stmt.setString(16, "01080209");//"TERM_ID,
                    stmt.setString(17, "898110248990017");//MCHNT_CD,
                    stmt.setString(18, "北京某某科技有限公司");//CARD_ACCPTR_NM_LOC,
                    stmt.setString(19, "00049996");//ACQ_INS_ID_CD,
                    stmt.setString(20, "00049992");//FWD_INS_ID_CD, " +

                    // 01025800|01020000|044103|1021124550|0000|
                    /**
                     * RCV_INS_ID_CD	接收机构标识码	C	13	√
                     ISS_INS_ID_CD	发卡机构标识码	C	13	√
                     ORIG_SYS_TRA_NO	原始系统跟踪号	C	6	√
                     ORIG_TRANSMSN_DT_TM		原始系统日期时	C	10	√
                     RSN_CD	原因码	C	4	√
                     */
                    stmt.setString(21, "01025800");//"RCV_INS_ID_CD,
                    stmt.setString(22, "01020000");//ISS_INS_ID_CD,
                    stmt.setString(23, "044103");//ORIG_SYS_TRA_NO,
                    stmt.setTimestamp(24, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));;// ORIG_TRANSMSN_DT_TM,
                    stmt.setString(25, "0000");//RSN_CD, " +

                    // /?|N|012|00|200000|
                    /**
                     * AUTH_DT		授权日期		C	4
                     AUTH_ID_RESP_CD	授权标识应答码	C	7	√
                     POS_ENTRY_MD_CD	服务点输入方式	C	3	√
                     POS_COND_CD	服务点条件代码	C	2	√
                     PROC_CD	交易处理码	C	6	√
                     */
                    stmt.setTimestamp(26, new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)));;//"AUTH_DT,
                    stmt.setString(27, "N");//AUTH_ID_RESP_CD,
                    stmt.setString(28, "012");//POS_ENTRY_MD_CD,
                    stmt.setString(29, "00");//POS_COND_CD,
                    stmt.setString(30, "200000");//PROC_CD, " +

                    // 000100|0
                    /**
                     * SUBINST_CODE	所属分支机构	C	6	√
                     INVALID	交易是否有效	C	1	√
                     */
                    stmt.setString(31, "000100"); //"SUBINST_CODE,
                    stmt.setString(32, "0"); // INVALID) " +

                    stmt.addBatch();
                    if(i%1000==0){
                        stmt.executeBatch();
//                        stmt.executeUpdate();
                    }
                }
                stmt.executeBatch();

            }
            Date endTime = new Date();
            System.out.println("开始插入：" + endTime);
            System.out.println("耗时（s）：" + (endTime.getTime() - startTime.getTime())/1000 );

//            print("Populated data.");
//
//            // Get data.
//            try (Statement stmt = conn.createStatement()) {
//                try (ResultSet rs =
//                    stmt.executeQuery("SELECT p.name, c.name FROM Person p INNER JOIN City c on c.id = p.city_id")) {
//                    print("Query results:");
//
//                    while (rs.next())
//                        System.out.println(">>>    " + rs.getString(1) + ", " + rs.getString(2));
//                }
//            }

            // Drop database objects.
//            try (Statement stmt = conn.createStatement()) {
//                stmt.executeUpdate("DROP TABLE Person");
//                stmt.executeUpdate("DROP TABLE City");
//            }

//            print("Dropped database objects.");
        }

        print("JDBC example finished.");
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }
//2106|20160831134258|20160831|592014|00|S30|1|32380|0220|0831134258|4899|245500440078|6222023********0000|622202|01|4|01080209|898110248990017|北京某某科技有限公司|00049996|00049992|01025800|01020000|044103|1021124550|0000||N|012|00|200000|000100|0

}