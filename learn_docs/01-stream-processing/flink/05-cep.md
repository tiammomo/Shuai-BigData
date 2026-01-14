# Flink CEP 复杂事件处理

## 目录

- [CEP 核心概念](#cep-核心概念)
- [模式定义](#模式定义)
- [模式检测](#模式检测)
- [输出结果](#输出结果)
- [应用场景](#应用场景)
- [最佳实践](#最佳实践)

---

## CEP 核心概念

### 什么是 CEP?

```
┌─────────────────────────────────────────────────────────────────┐
│                    CEP 核心概念                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Complex Event Processing (复杂事件处理)                         │
│                                                                 │
│  核心思想:                                                        │
│  - 从简单事件流中检测复杂模式                                     │
│  - 识别事件之间的关系                                             │
│  - 发现异常行为或趋势                                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                     事件流                               │   │
│  │  ──A──B──C──A──B──B──D──A──C──...──►                   │   │
│  │                     │                                    │   │
│  │                     ▼                                    │   │
│  │               ┌─────────┐                                │   │
│  │               │  CEP    │                                │   │
│  │               │  引擎   │                                │   │
│  │               └────┬────┘                                │   │
│  │                    │                                     │   │
│  │                    ▼                                     │   │
│  │  ────────────检测到模式──────────────────►               │   │
│  │  ───A──B──C──A──B──B──D──A──C──...──►                   │   │
│  │              ↑                                          │   │
│  │              └── 检测到: A+B+C 模式                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 流处理:                                                      │
│                                                                 │
│  | 特性        | 普通流处理     | CEP                         │   │
│  |------------|---------------|---------------------------|   │
│  | 数据处理    | 每条记录       | 基于模式匹配               │   │
│  | 时间窗口    | 固定窗口       | 事件序列窗口               │   │
│  | 聚合       | 数值聚合       | 事件关系聚合               │   │
│  | 输出       | 实时计算结果   | 匹配的事件序列             │   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Maven 依赖

```xml
<!-- Flink CEP -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep_2.12</artifactId>
    <version>${flink.version}</version>
</dependency>
```

---

## 模式定义

### 简单模式

```java
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern.*;
import java.util.List;
import java.util.Map;

/**
 * 模式定义详解
 */
public class PatternDefinition {

    /**
     * 1. 严格连续 (next)
     *
     * 事件必须严格连续，中间不能有其他事件
     */
    public static Pattern<LoginEvent, ?> strictSequential() {
        return Pattern
            .<LoginEvent>begin("first")
            .next("second")
            .next("third")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            });
    }

    /**
     * 2. 宽松连续 (followedBy)
     *
     * 允许中间有其他事件
     */
    public static Pattern<LoginEvent, ?> relaxedSequential() {
        return Pattern
            .<LoginEvent>begin("first")
            .followedBy("second")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            });
    }

    /**
     * 3. 非确定性宽松连续 (followedByAny)
     *
     * 允许重叠匹配
     */
    public static Pattern<LoginEvent, ?> nonDeterministic() {
        return Pattern
            .<LoginEvent>begin("first")
            .followedByAny("second")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            });
    }

    /**
     * 4. 否定条件 (notNext, notFollowedBy)
     *
     * 排除特定模式
     */
    public static Pattern<LoginEvent, ?> negativePattern() {
        return Pattern
            .<LoginEvent>begin("first")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("SUCCESS");
                }
            })
            .notNext("failed")  // 不能紧跟失败
            .followedBy("third")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getType().equals("PASSWORD_CHANGE");
                }
            });
    }
}
```

### 量词

```java
/**
 * 量词配置
 */
public class Quantifiers {

    /**
     * 1. 次数 (times)
     */
    public static Pattern<LoginEvent, ?> times() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .times(3);  // 出现3次
            // .times(2, 4)  // 出现2-4次
    }

    /**
     * 2. 可选 (optional)
     */
    public static Pattern<LoginEvent, ?> optional() {
        return Pattern
            .<LoginEvent>begin("warning")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getType().equals("WARNING");
                }
            })
            .optional()  // 可选
            .followedBy("error")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getType().equals("ERROR");
                }
            });
    }

    /**
     * 3. 一次或多次 (oneOrMore)
     */
    public static Pattern<LoginEvent, ?> oneOrMore() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .oneOrMore();  // 一次或多次
    }

    /**
     * 4. 零次或多次 (timesOrMore)
     */
    public static Pattern<LoginEvent, ?> timesOrMore() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .timesOrMore(2);  // 至少2次
    }

    /**
     * 5. 贪婪模式 (greedy)
     *
     * 尽可能多地匹配
     */
    public static Pattern<LoginEvent, ?> greedy() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .oneOrMore()
            .greedy()  // 贪婪模式
            .followedBy("success")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("SUCCESS");
                }
            });
    }

    /**
     * 6. 连续 (consecutive)
     *
     * 与 oneOrMore 配合使用，要求严格连续
     */
    public static Pattern<LoginEvent, ?> consecutive() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .oneOrMore()
            .consecutive()  // 严格连续
            .followedBy("success")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("SUCCESS");
                }
            });
    }
}
```

### 条件组合

```java
/**
 * 条件组合
 */
public class ConditionCombination {

    /**
     * 1. AND 条件
     */
    public static Pattern<LoginEvent, ?> andCondition() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .and(  // AND 条件
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getAttempts() >= 3;
                    }
                }
            );
    }

    /**
     * 2. OR 条件
     */
    public static Pattern<LoginEvent, ?> orCondition() {
        return Pattern
            .<LoginEvent>begin("event")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getType().equals("ERROR");
                }
            })
            .or(  // OR 条件
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getType().equals("WARNING");
                    }
                }
            );
    }

    /**
     * 3. 迭代条件 (复杂计算)
     */
    public static Pattern<LoginEvent, ?> iterativeCondition() {
        return Pattern
            .<LoginEvent>begin("failed")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .followedBy("failed2")
            .where(new IterativeCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event,
                                       Context<LoginEvent> ctx) throws Exception {
                    if (!event.getStatus().equals("FAILED")) {
                        return false;
                    }

                    // 迭代检查: 累计失败次数不超过5次
                    int totalFailures = 1;
                    for (LoginEvent e : ctx.getEventsForPattern("failed")) {
                        totalFailures++;
                    }
                    return totalFailures <= 5;
                }
            });
    }

    /**
     * 4. 组合条件
     */
    public static Pattern<LoginEvent, ?> combinedCondition() {
        return Pattern
            .<LoginEvent>begin("login")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getAction().equals("LOGIN");
                }
            })
            .followedBy("password_error")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getAction().equals("PASSWORD_ERROR");
                }
            })
            .followedBy("password_error2")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getAction().equals("PASSWORD_ERROR");
                }
            })
            .optional()  // 可选
            .followedBy("success")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getAction().equals("LOGIN_SUCCESS");
                }
            })
            .within(java.time.Duration.ofMinutes(30));  // 30分钟内
    }
}
```

---

## 模式检测

### 检测流程

```java
import org.apache.flink.cep.nfa.afterafterskippingpolicy.NAryAfterSkippingPolicy;
import org.apache.flink.cep.pattern.ContinuousPattern;
import org.apache.flink.cep.pattern.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.PatternSelectFunction;
import org.apache.flink.cep.pattern.PatternTimeoutFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 模式检测详解
 */
public class PatternDetection {

    /**
     * 1. 基本检测
     */
    public static void basicDetection() {
        DataStream<LoginEvent> loginStream = env
            .fromCollection(getLoginEvents())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5)
                ).withTimestampAssigner(
                    (event, ts) -> event.getTimestamp()
                )
            );

        // 定义模式
        Pattern<LoginEvent, ?> pattern = Pattern
            .<LoginEvent>begin("first")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .next("second")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .next("third")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("SUCCESS");
                }
            })
            .within(Time.minutes(30));

        // 应用模式
        DataStream<LoginAlert> alerts = CEP
            .pattern(loginStream, pattern)
            .process(
                new PatternSelectFunction<LoginEvent, LoginAlert>() {
                    @Override
                    public LoginAlert select(Map<String, List<LoginEvent>> pattern) {
                        LoginEvent first = pattern.get("first").get(0);
                        LoginEvent second = pattern.get("second").get(0);
                        LoginEvent third = pattern.get("third").get(0);

                        return new LoginAlert(
                            first.getUserId(),
                            "多次失败后成功登录",
                            first.getTimestamp()
                        );
                    }
                }
            );

        alerts.print();
    }

    /**
     * 2. 超时检测 (带有窗口)
     */
    public static void timeoutDetection() {
        DataStream<LoginEvent> loginStream = env
            .fromCollection(getLoginEvents())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5)
                ).withTimestampAssigner(
                    (event, ts) -> event.getTimestamp()
                )
            );

        Pattern<LoginEvent, ?> pattern = Pattern
            .<LoginEvent>begin("first")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("FAILED");
                }
            })
            .followedBy("second")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return event.getStatus().equals("SUCCESS");
                }
            })
            .within(Time.minutes(10));

        // 带超时的模式检测
        DataStream<LoginAlert> alerts = CEP
            .pattern(loginStream, pattern)
            .process(
                new PatternSelectFunction<LoginEvent, LoginAlert>() {
                    @Override
                    public LoginAlert select(Map<String, List<LoginEvent>> pattern) {
                        LoginEvent first = pattern.get("first").get(0);
                        LoginEvent second = pattern.get("second").get(0);

                        return new LoginAlert(
                            first.getUserId(),
                            "失败后成功登录",
                            first.getTimestamp()
                        );
                    }
                },
                new PatternTimeoutFunction<LoginEvent, LoginAlert>() {
                    @Override
                    public LoginAlert timeout(Map<String, List<LoginEvent>> pattern,
                                               long timeoutTimestamp) {
                        LoginEvent first = pattern.get("first").get(0);

                        return new LoginAlert(
                            first.getUserId(),
                            "登录超时预警",
                            timeoutTimestamp
                        );
                    }
                }
            );

        alerts.print();
    }

    /**
     * 3. 循环模式检测
     */
    public static void loopingPatternDetection() {
        DataStream<TransactionEvent> transactionStream = env
            .fromCollection(getTransactionEvents())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5)
                ).withTimestampAssigner(
                    (event, ts) -> event.getTimestamp()
                )
            );

        Pattern<TransactionEvent, ?> pattern = Pattern
            .<TransactionEvent>begin("small")
            .where(new SimpleCondition<TransactionEvent>() {
                @Override
                public boolean filter(TransactionEvent event) {
                    return event.getAmount() < 100;
                }
            })
            .followedBy("large")
            .where(new SimpleCondition<TransactionEvent>() {
                @Override
                public boolean filter(TransactionEvent event) {
                    return event.getAmount() > 10000;
                }
            })
            .within(Time.hours(1));

        // 检测模式
        CEP.pattern(transactionStream, pattern)
            .process(new PatternSelectFunction<TransactionEvent, FraudAlert>() {
                @Override
                public FraudAlert select(Map<String, List<TransactionEvent>> pattern) {
                    List<TransactionEvent> small = pattern.get("small");
                    TransactionEvent large = pattern.get("large").get(0);

                    double totalSmall = small.stream()
                        .mapToDouble(TransactionEvent::getAmount)
                        .sum();

                    return new FraudAlert(
                        large.getUserId(),
                        "可疑交易模式: 小额转大额",
                        totalSmall,
                        large.getAmount(),
                        large.getTimestamp()
                    );
                }
            });
    }
}
```

---

## 输出结果

### 结果处理

```java
/**
 * 结果处理
 */
public class ResultProcessing {

    /**
     * 1. 扁平化输出 (flatSelect)
     */
    public static void flatSelect() {
        CEP.pattern(loginStream, pattern)
            .flatSelect(
                new PatternFlatSelectFunction<LoginEvent, LoginAlert>() {
                    @Override
                    public void flatSelect(Map<String, List<LoginEvent>> pattern,
                                           Collector<LoginAlert> out) {
                        LoginEvent first = pattern.get("first").get(0);
                        LoginEvent second = pattern.get("second").get(0);

                        // 生成多个告警
                        out.collect(new LoginAlert(
                            first.getUserId(),
                            "告警1: 异常登录",
                            first.getTimestamp()
                        ));
                        out.collect(new LoginAlert(
                            first.getUserId(),
                            "告警2: 安全通知",
                            second.getTimestamp()
                        ));
                    }
                }
            );
    }

    /**
     * 2. 迭代条件结果
     */
    public static void iterativeSelect() {
        CEP.pattern(orderStream, pattern)
            .select(new PatternSelectFunction<OrderEvent, OrderPattern>() {
                @Override
                public OrderPattern select(Map<String, List<OrderEvent>> pattern) {
                    // 获取所有匹配的事件
                    List<OrderEvent> events = pattern.get("failed");

                    // 计算失败次数
                    int failureCount = events.size();

                    // 计算时间跨度
                    long firstTime = events.get(0).getTimestamp();
                    long lastTime = events.get(events.size() - 1).getTimestamp();

                    return new OrderPattern(
                        events.get(0).getUserId(),
                        failureCount,
                        lastTime - firstTime
                    );
                }
            });
    }

    /**
     * 3. 组合输出
     */
    public static void combinedOutput() {
        // 组合多个 CEP 结果
        DataStream<Alert> alerts1 = CEP.pattern(stream1, pattern1)
            .process(new AlertSelectFunction());

        DataStream<Alert> alerts2 = CEP.pattern(stream2, pattern2)
            .process(new AlertSelectFunction());

        // 合并告警
        alerts1.union(alerts2)
            .keyBy(Alert::getUserId)
            .process(new AggregateAlertsFunction());
    }
}
```

---

## 应用场景

### 场景示例

```java
/**
 * CEP 应用场景
 */
public class CEPApplications {

    /**
     * 1. 欺诈检测
     */
    public static class FraudDetection {

        /**
         * 检测洗钱模式: 多账户快速转账
         */
        public static Pattern<TransactionEvent, ?> moneyLaunderingPattern() {
            return Pattern
                .<TransactionEvent>begin("source")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.getType().equals("DEPOSIT");
                    }
                })
                .followedBy("transfer")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.getType().equals("TRANSFER");
                    }
                })
                .oneOrMore()
                .followedBy("withdraw")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.getType().equals("WITHDRAW");
                    }
                })
                .within(Time.hours(24));
        }
    }

    /**
     * 2. 异常检测
     */
    public static class AnomalyDetection {

        /**
         * 检测服务器异常: CPU 持续高负载
         */
        public static Pattern<ServerMetric, ?> highCpuPattern() {
            return Pattern
                .<ServerMetric>begin("high")
                .where(new SimpleCondition<ServerMetric>() {
                    @Override
                    public boolean filter(ServerMetric metric) {
                        return metric.getCpuUsage() > 90;
                    }
                })
                .followedBy("still_high")
                .where(new SimpleCondition<ServerMetric>() {
                    @Override
                    public boolean filter(ServerMetric metric) {
                        return metric.getCpuUsage() > 85;
                    }
                })
                .times(3)
                .consecutive()
                .within(Time.minutes(5));
        }
    }

    /**
     * 3. 用户行为分析
     */
    public static class UserBehaviorAnalysis {

        /**
         * 检测用户会话: 搜索 -> 浏览 -> 加购 -> 下单
         */
        public static Pattern<UserAction, ?> purchasePattern() {
            return Pattern
                .<UserAction>begin("search")
                .where(new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction action) {
                        return action.getAction().equals("SEARCH");
                    }
                })
                .followedBy("browse")
                .where(new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction action) {
                        return action.getAction().equals("BROWSE");
                    }
                })
                .followedBy("cart")
                .where(new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction action) {
                        return action.getAction().equals("ADD_TO_CART");
                    }
                })
                .followedBy("purchase")
                .where(new SimpleCondition<UserAction>() {
                    @Override
                    public boolean filter(UserAction action) {
                        return action.getAction().equals("PURCHASE");
                    }
                })
                .within(Time.hours(1));
        }
    }

    /**
     * 4. 告警规则
     */
    public static class AlertRules {

        /**
         * 检测多次登录失败
         */
        public static Pattern<LoginEvent, ?> loginFailurePattern() {
            return Pattern
                .<LoginEvent>begin("failed")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getStatus().equals("FAILED");
                    }
                })
                .next("failed2")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getStatus().equals("FAILED");
                    }
                })
                .next("failed3")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getStatus().equals("FAILED");
                    }
                })
                .within(Time.minutes(15));
        }
    }
}
```

---

## 最佳实践

### 性能优化

```java
/**
 * CEP 性能优化
 */
public class CEPOptimization {

    /**
     * 1. 避免复杂的迭代条件
     *
     * 复杂迭代条件会影响性能
     */
    public static void avoidComplexConditions() {
        // 不好: 每次匹配都迭代
        Pattern.<Event>begin("first")
            .followedBy("second")
            .where(new IterativeCondition<Event>() {
                @Override
                public boolean filter(Event event, Context<Event> ctx) {
                    // 复杂计算
                    for (Event e : ctx.getEventsForPattern("first")) {
                        // 迭代检查
                    }
                    return false;
                }
            });

        // 好: 使用量词代替
        Pattern.<Event>begin("first")
            .where(condition)
            .times(3)
            .followedBy("second")
            .where(condition);
    }

    /**
     * 2. 合理设置窗口时间
     *
     * 窗口太大会影响性能和内存
     */
    public static void reasonableWindow() {
        // 根据业务需求设置合理窗口
        Pattern.<LoginEvent, ?> pattern = Pattern
            .<LoginEvent>begin("first")
            .followedBy("second")
            .where(condition)
            .within(Time.minutes(30));  // 根据实际场景调整
    }

    /**
     * 3. 使用适当的时间语义
     *
     * 事件时间 vs 处理时间
     */
    public static void appropriateTimeSemantics() {
        // 使用事件时间，处理乱序事件
        DataStream<LoginEvent> stream = env
            .fromCollection(events)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5)
                ).withTimestampAssigner(
                    (event, ts) -> event.getTimestamp()
                )
            );

        Pattern<LoginEvent, ?> pattern = Pattern
            .<LoginEvent>begin("first")
            .where(condition)
            .followedBy("second")
            .where(condition)
            .within(Time.minutes(30));
    }

    /**
     * 4. 监控 CEP 性能
     */
    public static void monitorPerformance() {
        // 关键指标:
        // - cep.patternLatency: 模式匹配延迟
        // - cep.throughput: 处理吞吐量
        // - cep.nfaStates: NFA 状态数量

        // 告警配置
        // alert if cep.patternLatency > 1s
        // alert if cep.throughput < 1000 events/s
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Flink 架构详解](01-architecture.md) | Flink 核心架构和概念 |
| [Flink DataStream API](02-datastream.md) | DataStream API 详解 |
| [Flink Table API / SQL](03-table-sql.md) | Table API 和 SQL 详解 |
| [Flink 状态管理](04-state-checkpoint.md) | 状态管理和 Checkpoint |
| [Flink 运维指南](06-operations.md) | 集群部署和运维 |
