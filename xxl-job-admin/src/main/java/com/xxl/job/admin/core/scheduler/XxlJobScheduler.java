package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // init i18n
        // i18n 国际化；i18n是 Internationalization 这个英文的简写，国际化的意思
        initI18n();

        // admin trigger pool start
        // 快慢触发器线程池初始化，快慢线程池有大用
        // 调度中心JobTriggerPoolHelper
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run
        // 监控线程开始执行监控
        // 至少30秒执行一次,维护注册表信息，通过表数据判断在线超时时间90s
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run
        // 运行事变监视器,主要失败发送邮箱,重试触发器
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        //将丢失主机信息调度日志更改状态
        JobCompleteHelper.getInstance().start();

        // admin log report start
        //统计一些失败成功报表
        JobLogReportHelper.getInstance().start();

        /*
        * 以数据库表的形式存放各种数据
        * 包括：
        * 节点数据、节点心跳数据
        * 事件执行失败告警数据
        * 调度日志数据
        * 调度器执行数据等
        * */

        // start-schedule  ( depend on JobTriggerPoolHelper )
        // 具体任务调度JobScheduleHelper
        // 执行核心

        // start-schedule
        // 创建2个线程 :scheduleThread, ringThread

        // scheduleThread :
        // 获取可处理数量(trigger中快慢两个线程池中的最大线程数相加 * qps 目前xxl计算出来qps=20)
        // 使用xxl_job_lock表中的lock_name=schedule_lock的数据进行分布式排他锁，
        // 根据当前时间，查询出来在xxl_job_info表中下次执行时间小于当前时间的所有job,数量为之前获取的可处理数量。

        // ringThread:
        // 基于秒级的任务调试中心
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
