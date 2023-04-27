package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.thread.JobScheduleHelper;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author charles.xu
 * @ProjectName xxl-job-master
 * @description XxlJobSchedulerCopy -
 * @time 2023/4/27 15:39
 */
public class XxlJobSchedulerCopy {

    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();
    public static JobScheduleHelper getInstance(){
        return instance;
    }

    public static final long PRE_READ_MS = 5000;    // pre read

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    public void start(){

        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    //下5秒之后执行一次，等待服务器启动。
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis()%1000 );
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                // pre-read count: treadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)  每个触发器花费50ms,每个线程单位时间内处理20任务,最多同时处理300*20=6000任务
                int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

                while (!scheduleThreadToStop) {

                    // Scan Job
                    // 记录执行一次循环花费时间
                    long start = System.currentTimeMillis();

                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;

                    boolean preReadSuc = true;
                    try {
                        //设置手动提交
                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);
                        //获取任务调度锁表内数据信息,加写锁
                        preparedStatement = conn.prepareStatement(  "select * from xxl_job_lock where lock_name = 'schedule_lock' for update" );
                        preparedStatement.execute();

                        // tx start

                        // 1、pre read
                        long nowTime = System.currentTimeMillis();   //获取当前时间后5秒,同时最多负载的分页数
                        // triggerNextTime <= nowTime + PRE_READ_MS --->>> nowTime >= TriggerNextTime - PRE_READ_MS
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        if (scheduleList!=null && scheduleList.size()>0) {
                            // 2、push time-ring
                            for (XxlJobInfo jobInfo: scheduleList) {

                                // time-ring jump
                                // 执行时间 + 5s < 当前时间 -> 执行时间 < 当前时间 - 5s
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {  //触发器过期时间>5s
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                    logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());

                                    // 1、misfire match
//                                    - 调度过期策略：
//                                    - 忽略：调度过期后，忽略过期的任务，从当前时间开始重新计算下次触发时间；
//                                    - 立即执行一次：调度过期后，立即执行一次，并从当前时间开始重新计算下次触发时间；
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) { //若过期策略为FIRE_ONCE_NOW,则立即执行一次
                                        // FIRE_ONCE_NOW -> trigger //执行触发器
                                        JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId() );
                                    }

                                    // 2、fresh next  更新下次执行时间
                                    refreshNextValidTime(jobInfo, new Date());

                                } else if (nowTime > jobInfo.getTriggerNextTime()) { //触发器过期时间<5s
                                    /// 执行时间 < 当前时间，&& 执行的时间 >= 当前时间 - 5s
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time

                                    // 1、trigger //执行触发器
                                    // 添加到触发器中执行（异步）
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId() );

                                    // 2、fresh next 更新下次执行时间
                                    refreshNextValidTime(jobInfo, new Date());

                                    // next-trigger-time in 5s, pre-read again 下次触发时间在当前时间往后5秒范围内
                                    if (jobInfo.getTriggerStatus()==1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                                        // 已经更新后的下次执行时间 < 当前时间 + 5s
                                        // 1、make ring second 获取下次执行秒
                                        int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                        // 2、push time ring
                                        // 放到ring线程中执行 - 时间轮中执行
                                        // ringData是一个ConcurrentHashMap,key是-秒级时间， value-是执行的任务jobId的集合
                                        pushTimeRing(ringSecond, jobInfo.getId());

                                        // 3、fresh next 更新下次执行时间
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                    }

                                } else {
                                    // 执行时间 >= 当前时间 && 执行时间 <= 当前时间 + 5s
                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time
                                    //未来五秒以内执行的所有任务添加到ringData
                                    // 1、make ring second 获取下次执行秒
                                    int ringSecond = (int)((jobInfo.getTriggerNextTime()/1000)%60);

                                    // 2、push time ring
                                    // 放到ring线程中执行 - 时间轮中执行
                                    pushTimeRing(ringSecond, jobInfo.getId());

                                    // 3、fresh next 更新下次执行时间
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                }

                            }

                            // 3、update trigger info 更新执行时间和上次执行时间到数据库
                            for (XxlJobInfo jobInfo: scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        } else {
                            // 查询不到数据
                            preReadSuc = false;
                        }

                        // tx stop


                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {

                        // commit
                        // 释放锁
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis()-start;


                    // Wait seconds, align second
                    if (cost < 1000) {  // scan-overtime, not wait
                        // 执行耗时 < 1s
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            //若执行成功,下一秒继续执行。执行失败或没查询出数据则5秒执行一次。
                            // 执行时间少于1S
                            // 有调度任务:1s之内随机sleep;
                            // 无调度任务: 5s之内随机sleep; 为什么是5s之内？因为调度任务是查的当前时间+5s的
                            // preReadSuc： 是否有调度任务
                            // PRE_READ_MS：5000
                            TimeUnit.MILLISECONDS.sleep((preReadSuc?1000:PRE_READ_MS) - System.currentTimeMillis()%1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }

                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        // 守护线程
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ring thread
        // 时间轮线程
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {

                while (!ringThreadToStop) {

                    // align second
                    try {
                        // 随机休眠
                        // 该随机休眠是在循环中，在1sec内随机休眠，避免频繁空循环
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();
                        // 获取当前秒
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);   // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                        for (int i = 0; i < 2; i++) {
                            // 实际只执行i = 0; i = 1的情况，循环2次
                            // private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();
                            // ringData是一个ConcurrentHashMap，key - 秒级时间 ， value - 待执行的jobId集合
                            //假设现在为1秒，那么执行任务之后，5秒之后的任务分别会添加到23456下标位置。
                            //  i=0; (1+60-0) % 60 = 1
                            // i=1：（1+60-1）%60=0
                            // i=2：（1+60-2）%60=59
                            // 这里实质上已经是向前跨越了2个刻度执行
                            List<Integer> tmpData = ringData.remove( (nowSecond+60-i)%60 );
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }

                        // ring trigger
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData) );
                        if (ringItemData.size() > 0) {
                            // do trigger
                            for (int jobId: ringItemData) {
                                // do trigger //执行触发器
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }




    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            // generateNextValidTime() 返回null -> ScheduleTypeEnum类型为NONE
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.warn(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }

    private void pushTimeRing(int ringSecond, int jobId){
        // push async ring
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData) );
    }

    public void toStop(){

        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData!=null && tmpData.size()>0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED){
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }


    // ---------------------- tools ----------------------
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf())*1000 );
        }
        return null;
    }



}
