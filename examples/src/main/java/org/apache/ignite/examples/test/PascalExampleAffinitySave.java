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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example to showcase DDL capabilities of Ignite's SQL engine.
 * <p>
 * Remote nodes could be started from command line as follows:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in either same or another JVM.
 */
public class PascalExampleAffinitySave {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";
    public static AtomicLong atomicLong = new AtomicLong(1);
    public static int TOTAL_RECORD = 10*100;
    public static int CONNECTOR_NUM = 10;
    public static AtomicInteger connectorCounter = new AtomicInteger(CONNECTOR_NUM -1);
    public static Date startTime = null;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    @SuppressWarnings({"unused", "ThrowFromFinallyBlock"})
    public static void main(String[] args) throws Exception {
        PascalExampleAffinitySave pascalExample = new PascalExampleAffinitySave();
        Ignition.setClientMode(false);
        try (Ignite ignite = Ignition.start("examples/config/zookeeper-config.xml")) {


            // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
            // will appear in future versions, JDBC and ODBC drivers do not require it already).
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");
            IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg);

            pascalExample.createTable(cache);
            startTime = new Date();

            for(int i=1;i<=CONNECTOR_NUM;i++){
                final int sn = i;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        pascalExample.insertData(cache,sn);
                    }
                }).start();
            }



            System.out.printf("Usage: " +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }
            }
            print("Cache query DDL example finished.");
        }
    }


    public void createTable(IgniteCache<?, ?> cache  ){
        print("Cache query DDL example started.");


            // drop table
//            cache.query(new SqlFieldsQuery(
//                    "drop TABLE if exists tran")).getAll();
            // Create reference City table based on REPLICATED template.
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE if NOT EXISTS tran(\n" +
                            "id\tlong\t,\n" +
                            "create_tm\ttimestamp\t,\n" +
                            "settle_dt\ttimestamp\t,\n" +
                            "sys_tra_no\tvarchar\t,\n" +
                            "trans_id\tvarchar\t,\n" +
                            "settle_in\tvarchar\t,\n" +
                            "trans_at\tlong\t,\n" +
                            "msg_tp\tvarchar\t,\n" +
                            "loc_trans_dt_tm\tvarchar\t,\n" +
                            "mchnt_tp\tvarchar\t,\n" +
                            "retri_ref_no\tvarchar\t,\n" +
                            "pri_acct_no\tvarchar\t,\n" +
                            "bin\tvarchar\t,\n" +
                            "card_attr\tvarchar\t,\n" +
                            "card_media\tvarchar\t,\n" +
                            "term_id\tvarchar\t,\n" +
                            "mchnt_cd\tvarchar\t,\n" +
                            "card_accptr_nm_loc\tvarchar\t,\n" +
                            "acq_ins_id_cd\tvarchar\t,\n" +
                            "fwd_ins_id_cd\tvarchar\t,\n" +
                            "rcv_ins_id_cd\tvarchar\t,\n" +
                            "iss_ins_id_cd\tvarchar\t,\n" +
                            "orig_sys_tra_no\tvarchar\t,\n" +
                            "orig_transmsn_dt_tm\ttimestamp\t,\n" +
                            "rsn_cd\tvarchar\t,\n" +
                            "auth_dt\ttimestamp\t,\n" +
                            "auth_id_resp_cd\tvarchar\t,\n" +
                            "pos_entry_md_cd\tvarchar\t,\n" +
                            "pos_cond_cd\tvarchar\t,\n" +
                            "proc_cd\tvarchar\t,\n" +
                            "subinst_code\tvarchar\t,\n" +
                            "invalid\tvarchar\t,\n" +
                            "PRIMARY key (id,pri_acct_no)\n" +
                            ") WITH \"template=partitioned, backups=1, affinityKey=pri_acct_no, key_type=TranKey \" ")).getAll();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //CREATE index
        cache.query(new SqlFieldsQuery("CREATE index if NOT EXISTS idx_pri_acct_no ON tran(pri_acct_no)")).getAll();


        print("Created database objects.");
    }

    public void insertData( IgniteCache<?, ?> cache, int sn ){

            print("开始线程：" + sn);
            long start = System.currentTimeMillis();

            SqlFieldsQuery qry = new SqlFieldsQuery(
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
                            "?, ?)"
            );

            for(int i=0;i<TOTAL_RECORD/CONNECTOR_NUM;i++){
                qry.setArgs(
                        atomicLong.getAndIncrement(),
                        new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)),
                        new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000)),
                        "592014",
                        "S30"

                        // 1|32380|0220|0831134258|4899|
                        /**
                         * SETTLE_IN	清算标志	C	1	√
                         TRANS_AT	交易金额	N	12	√
                         MSG_TP	消息类型	C	4	√
                         LOC_TRANS_DT_TM	C	10
                         MCHNT_TP	C	4	√
                         */
                        , 1
                        , new Integer(1000 + (int)(Math.random()* 10000)).toString()
                        , "0220"
                        ,"0831134258"
                        ,"4899"


                        // 245500440078|6222023********0000|622202|01|4|
                        /**
                         * RETRI_REF_NO	检索参考号	C	12	√
                         PRI_ACCT_NO	主帐号	C	21	√
                         BIN	卡	bin(含长度)	C	14
                         CARD_ATTR	卡性质	C	2	√
                         CARD_MEDIA	卡介质	C	1	√
                         */
                        , "245500440078"
                        , "6222023000000000000"+new Integer(100 + (int)(Math.random()*100)).toString()
                        , "622202"
                        ,"01"
                        , "4"


                        // 01080209|898110248990017|北京某某科技有限公司|00049996|00049992|
                        /**
                         * TERM_ID	C	8
                         MCHNT_CD	C	15
                         CARD_ACCPTR_NM_LOC	C	40	√
                         ACQ_INS_ID_CD	受理机构标识码	C	13	√
                         FWD_INS_ID_CD	转发机构标识码	C	13	√
                         */
                        , "01080209"
                        ,"898110248990017"
                        ,"北京某某科技有限公司"
                        ,"00049996"
                        ,"00049992"

                        // 01025800|01020000|044103|1021124550|0000|
                        /**
                         * RCV_INS_ID_CD	接收机构标识码	C	13	√
                         ISS_INS_ID_CD	发卡机构标识码	C	13	√
                         ORIG_SYS_TRA_NO	原始系统跟踪号	C	6	√
                         ORIG_TRANSMSN_DT_TM		原始系统日期时	C	10	√
                         RSN_CD	原因码	C	4	√
                         */
                    , "01025800"
                        , "01020000"
                        , "044103"
                        , new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000))
                        , "0000"


                        // /?|N|012|00|200000|
                        /**
                         * AUTH_DT		授权日期		C	4
                         AUTH_ID_RESP_CD	授权标识应答码	C	7	√
                         POS_ENTRY_MD_CD	服务点输入方式	C	3	√
                         POS_COND_CD	服务点条件代码	C	2	√
                         PROC_CD	交易处理码	C	6	√
                         */
                        , new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000))
                        , "N"
                        , "012"
                        , "00"
                        ,  "200000"
                        // 000100|0
                        /**
                         * SUBINST_CODE	所属分支机构	C	6	√
                         INVALID	交易是否有效	C	1	√
                         */
                        ,"000100"
                        ,"0"



                );
                cache.query(qry).getAll();

            }
            Date endDate = new Date();
            print("线程执行结束：" + sn);
            print("当前用时：" + ((endDate.getTime() - startTime.getTime())  ) + "ms.");

        int andDecrement = connectorCounter.getAndDecrement();
        print("剩余的线程数量是：" + andDecrement);


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
}
