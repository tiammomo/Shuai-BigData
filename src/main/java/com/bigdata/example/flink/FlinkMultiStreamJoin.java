package com.bigdata.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flink多流Join详细示例
 * 包含: Window Join, Interval Join, Connect/CoProcess等
 */
public class FlinkMultiStreamJoin {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("   Flink多流Join完整示例");
        System.out.println("========================================\n");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // ==================== Window Join ====================
        System.out.println("【Window Join】基于窗口的Join");
        windowJoinExample(env);

        // ==================== Interval Join ====================
        System.out.println("\n【Interval Join】基于时间区间的Join");
        intervalJoinExample(env);

        // ==================== CoProcess/CoFlatMap ====================
        System.out.println("\n【CoProcessFunction】低-level流处理");
        coProcessExample(env);

        // ==================== 双流Connect ====================
        System.out.println("\n【双流Connect】流的连接与协同处理");
        dualStreamConnect(env);

        // ==================== 多流Union ====================
        System.out.println("\n【多流Union】合并多个同类型流");
        multiStreamUnion(env);

        // ==================== 实时对账系统 ====================
        System.out.println("\n【实战案例】实时对账系统");
        reconciliationSystem(env);

        // ==================== 双流Full Outer Join ====================
        System.out.println("\n【Full Outer Join】双流全外连接");
        fullOuterJoinExample(env);

        System.out.println("\n========================================");
        System.out.println("   Flink多流Join示例执行完成!");
        System.out.println("========================================");
    }

    // ==================== Window Join ====================

    /**
     * Window Join原理:
     * 1. 将两个流按Key分组
     * 2. 在相同的窗口内进行Join
     * 3. 支持滚动窗口和滑动窗口
     *
     * 特点:
     * - 要求两个流有相同的时间语义
     * - 窗口内的数据会两两组合
     * - 支持Inner Join和Full Outer Join
     */
    private static void windowJoinExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- Window Join 示例 ---");

        // 订单流
        DataStream<Tuple2<String, Double>> orders = env.fromElements(
                Tuple2.of("user1", 100.0),
                Tuple2.of("user1", 200.0),
                Tuple2.of("user2", 150.0),
                Tuple2.of("user3", 300.0)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Double>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
        );

        // 支付流
        DataStream<Tuple2<String, Double>> payments = env.fromElements(
                Tuple2.of("user1", 100.0),
                Tuple2.of("user1", 200.0),
                Tuple2.of("user2", 150.0),
                Tuple2.of("user4", 400.0)  // 没有对应的订单
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Double>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
        );

        // Window Join (Inner Join)
        DataStream<String> joinResult = orders
                .join(payments)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply((order, payment, out) -> {
                    out.collect("Order & Payment matched: user=" + order.f0 +
                               ", orderAmt=" + order.f1 + ", payAmt=" + payment.f1);
                });

        System.out.println("Window Join类型:");
        System.out.println("  1. orders.join(payments) - 内部连接(inner join)");
        System.out.println("  2. orders.fullOuterJoin(payments) - 全外连接");
        System.out.println("  3. orders.leftOuterJoin(payments) - 左外连接");
        System.out.println();
        System.out.println("Window Join限制:");
        System.out.println("  - 需要相同的时间语义");
        System.out.println("  - 窗口内的数据会笛卡尔积");
        System.out.println("  - 大数据量时需注意内存");
    }

    // ==================== Interval Join ====================

    /**
     * Interval Join原理:
     * - 基于事件时间的区间Join
     * - 不需要同一个窗口内的数据
     * - 更灵活的时间控制

     * 公式: left.ts + [lowerBound, upperBound] 包含 right.ts
     */
    private static void intervalJoinExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- Interval Join 示例 ---");

        // 点击事件流
        DataStream<Tuple3<String, Long, String>> clicks = env.fromElements(
                Tuple3.of("user1", 1000L, "click"),
                Tuple3.of("user1", 2500L, "click"),
                Tuple3.of("user2", 3000L, "click")
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> e.f1)
        );

        // 购买事件流
        DataStream<Tuple3<String, Long, String>> purchases = env.fromElements(
                Tuple3.of("user1", 2000L, "purchase"),
                Tuple3.of("user1", 4000L, "purchase"),
                Tuple3.of("user2", 3500L, "purchase")
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> e.f1)
        );

        // Interval Join: 点击后1-3秒内的购买
        DataStream<String> intervalJoinResult = clicks
                .keyBy(t -> t.f0)
                .intervalJoin(purchases.keyBy(t -> t.f0))
                .between(Duration.ofSeconds(1), Duration.ofSeconds(3))
                .upperBoundExclusive()  // 上界 exclusive
                .process((left, right, out) -> {
                    out.collect("Interval Join: " + left.f0 +
                               " clicked at " + left.f1 +
                               " and purchased at " + right.f1);
                });

        System.out.println("Interval Join配置:");
        System.out.println("  1. between(lower, upper) - 设置时间区间");
        System.out.println("  2. lowerBoundExclusive() - 下界 exclusive");
        System.out.println("  3. upperBoundExclusive() - 上界 exclusive");
        System.out.println();
        System.out.println("Interval Join vs Window Join:");
        System.out.println("  - Interval Join: 更灵活，基于事件时间区间");
        System.out.println("  - Window Join: 需要在同一个窗口内");
    }

    // ==================== CoProcessFunction ====================

    /**
     * CoProcessFunction:
     * - 最低层次的流处理API
     * - 可以完全控制两个流的数据
     * - 可以使用Timer定时器
     * - 可以使用状态存储中间结果
     */
    private static void coProcessExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- CoProcessFunction 示例 ---");

        // 订单流
        DataStream<Tuple2<String, Double>> orders = env.fromElements(
                Tuple2.of("user1", 100.0),
                Tuple2.of("user2", 200.0),
                Tuple2.of("user1", 150.0),
                Tuple2.of("user3", 300.0)
        );

        // 优惠券流
        DataStream<Tuple2<String, Double>> coupons = env.fromElements(
                Tuple2.of("user1", 20.0),
                Tuple2.of("user2", 30.0)
        );

        // 使用CoProcessFunction
        DataStream<String> result = orders
                .connect(coupons)
                .process(new CoProcessFunction<Tuple2<String, Double>, Tuple2<String, Double>, String>() {

                    // 状态: 缓存未匹配的订单
                    private transient ValueState<List<Tuple2<String, Double>>> pendingOrders;
                    // 状态: 缓存未使用的优惠券
                    private transient ValueState<List<Tuple2<String, Double>>> pendingCoupons;

                    @Override
                    public void open(Configuration parameters) {
                        pendingOrders = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pending-orders", List.class)
                        );
                        pendingCoupons = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pending-coupons", List.class)
                        );
                    }

                    @Override
                    public void processElement1(Tuple2<String, Double> order,
                                               Context ctx, Collector<String> out) throws Exception {
                        List<Tuple2<String, Double>> coupons = pendingCoupons.value();
                        if (coupons == null) coupons = new ArrayList<>();

                        // 查找匹配的优惠券
                        boolean matched = false;
                        List<Tuple2<String, Double>> remainingCoupons = new ArrayList<>();
                        for (Tuple2<String, Double> coupon : coupons) {
                            if (coupon.f0.equals(order.f0) && !matched) {
                                matched = true;
                                out.collect("Applied coupon: user=" + order.f0 +
                                           ", orderAmt=" + order.f1 +
                                           ", discount=" + coupon.f1 +
                                           ", finalAmt=" + (order.f1 - coupon.f1));
                            } else {
                                remainingCoupons.add(coupon);
                            }
                        }
                        pendingCoupons.update(remainingCoupons);

                        if (!matched) {
                            out.collect("No coupon: user=" + order.f0 +
                                       ", orderAmt=" + order.f1);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Double> coupon,
                                               Context ctx, Collector<String> out) throws Exception {
                        List<Tuple2<String, Double>> orders = pendingOrders.value();
                        if (orders == null) orders = new ArrayList<>();

                        // 缓存未使用的优惠券
                        orders.add(coupon);
                        pendingOrders.update(orders);

                        out.collect("Coupon cached: user=" + coupon.f0 +
                                   ", discount=" + coupon.f1);
                    }
                });

        System.out.println("CoProcessFunction方法:");
        System.out.println("  1. processElement1() - 处理第一个流的元素");
        System.out.println("  2. processElement2() - 处理第二个流的元素");
        System.out.println("  3. onTimer() - 定时器回调");
        System.out.println();
        System.out.println("使用场景:");
        System.out.println("  - 需要完全控制两个流的匹配逻辑");
        System.out.println("  - 需要使用Timer进行超时处理");
        System.out.println("  - 需要复杂的缓存和匹配策略");
    }

    // ==================== 双流Connect ====================

    /**
     * Connect vs Join:
     * - Connect: 保持两个流的独立性，可以独立处理
     * - Join: 必须是同一窗口内的数据
     *
     * Connect使用场景:
     * - 主从数据合并
     * - 规则匹配
     * - 数据补齐
     */
    private static void dualStreamConnect(StreamExecutionEnvironment env) {
        System.out.println("\n--- 双流Connect 示例 ---");

        // 主数据流
        DataStream<Tuple2<String, Integer>> mainStream = env.fromElements(
                Tuple2.of("user1", 100),
                Tuple2.of("user2", 200),
                Tuple2.of("user3", 300)
        );

        // 维度数据流
        DataStream<Tuple3<String, String, Integer>> dimStream = env.fromElements(
                Tuple3.of("user1", "VIP", 1),
                Tuple3.of("user2", "Normal", 2),
                Tuple3.of("user4", "New", 3)
        );

        // 使用KeyedCoProcessFunction进行连接
        DataStream<String> connectedResult = mainStream
                .connect(dimStream)
                .keyBy(t1 -> t1.f0, t2 -> t2.f0)
                .process(new KeyedCoProcessFunction<String,
                        Tuple2<String, Integer>,
                        Tuple3<String, String, Integer>,
                        String>() {

                    @Override
                    public void processElement1(Tuple2<String, Integer> value,
                                                Context ctx, Collector<String> out) {
                        out.collect("Main data: " + value.f0 + ", amount=" + value.f1);
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Integer> value,
                                                Context ctx, Collector<String> out) {
                        out.collect("Dim data: " + value.f0 + ", type=" + value.f1);
                    }
                });

        System.out.println("Connect vs Join:");
        System.out.println("  Connect: 两个流独立处理，可使用不同的处理逻辑");
        System.out.println("  Join: 必须在同一窗口内匹配");
        System.out.println();
        System.out.println("Connect使用场景:");
        System.out.println("  1. 主数据 + 维度数据合并");
        System.out.println("  2. 实时数据 + 规则/配置合并");
        System.out.println("  3. 业务流 + 控制流合并");
    }

    // ==================== 多流Union ====================

    /**
     * Union:
     * - 合并多个同类型的DataStream
     * - 所有流必须有相同的Schema
     * - 无法区分数据来源
     */
    private static void multiStreamUnion(StreamExecutionEnvironment env) {
        System.out.println("\n--- 多流Union 示例 ---");

        DataStream<String> stream1 = env.fromElements("A1", "A2", "A3");
        DataStream<String> stream2 = env.fromElements("B1", "B2");
        DataStream<String> stream3 = env.fromElements("C1", "C2", "C3", "C4");

        // Union合并多个流
        DataStream<String> unionStream = stream1.union(stream2, stream3);

        System.out.println("Union特点:");
        System.out.println("  1. 合并多个同类型流");
        System.out.println("  2. 无法区分数据来源");
        System.out.println("  3. 按事件时间排序");
        System.out.println();
        System.out.println("与Connect的区别:");
        System.out.println("  - Union: 同类型流，合并后不可区分来源");
        System.out.println("  - Connect: 不同类型流，独立处理");
    }

    // ==================== 实时对账系统 ====================

    /**
     * 实时对账系统案例:
     * - 订单流 vs 支付流
     * - 使用CoProcessFunction实现
     * - 支持超时和未匹配数据处理
     */
    private static void reconciliationSystem(StreamExecutionEnvironment env) {
        System.out.println("\n--- 实时对账系统案例 ---");

        // 定义侧输出流标签
        final OutputTag<String> unmatchedOrdersTag = new OutputTag<String>("unmatched-orders") {};
        final OutputTag<String> unmatchedPaymentsTag = new OutputTag<String>("unmatched-payments") {};
        final OutputTag<String> timeoutOrdersTag = new OutputTag<String>("timeout-orders") {};

        // 订单流
        DataStream<Tuple3<String, Double, Long>> orders = env.fromElements(
                Tuple3.of("order1", 100.0, 1000L),
                Tuple3.of("order2", 200.0, 2000L),
                Tuple3.of("order3", 150.0, 3000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> e.f2)
        );

        // 支付流
        DataStream<Tuple3<String, Double, Long>> payments = env.fromElements(
                Tuple3.of("pay1", 100.0, 1500L),
                Tuple3.of("pay2", 200.0, 2500L),
                Tuple3.of("pay3", 300.0, 4000L)  // 没有对应的订单
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> e.f2)
        );

        // 对账处理
        DataStream<String> reconciliationResult = orders
                .connect(payments)
                .keyBy(t -> t.f0.substring(0, 5), t -> t.f0.substring(0, 3))
                .process(new KeyedCoProcessFunction<String,
                        Tuple3<String, Double, Long>,
                        Tuple3<String, Double, Long>,
                        String>() {

                    private transient ValueState<Tuple3<String, Double, Long>> pendingOrder;
                    private transient ValueState<Tuple3<String, Double, Long>> pendingPayment;

                    @Override
                    public void open(Configuration parameters) {
                        pendingOrder = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pending-order", Tuple3.class)
                        );
                        pendingPayment = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pending-payment", Tuple3.class)
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, Double, Long> order,
                                                Context ctx, Collector<String> out) {
                        Tuple3<String, Double, Long> payment = pendingPayment.value();

                        if (payment != null && order.f0.substring(0, 3).equals(payment.f0.substring(0, 3))) {
                            // 匹配成功
                            out.collect("MATCHED: order=" + order.f0 +
                                       ", pay=" + payment.f0 +
                                       ", amount=" + order.f1);
                            pendingPayment.clear();
                        } else {
                            // 缓存订单，等待匹配
                            pendingOrder.update(order);
                            out.collect("Order pending: " + order.f0);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, Double, Long> payment,
                                                Context ctx, Collector<String> out) {
                        Tuple3<String, Double, Long> order = pendingOrder.value();

                        if (order != null && order.f0.substring(0, 3).equals(payment.f0.substring(0, 3))) {
                            // 匹配成功
                            out.collect("MATCHED: order=" + order.f0 +
                                       ", pay=" + payment.f0 +
                                       ", amount=" + order.f1);
                            pendingOrder.clear();
                        } else {
                            // 缓存支付，等待匹配
                            pendingPayment.update(payment);
                            out.collect("Payment pending: " + payment.f0);
                        }
                    }
                });

        System.out.println("对账系统要点:");
        System.out.println("  1. 使用CoProcessFunction实现流的对账");
        System.out.println("  2. 使用State缓存未匹配的数据");
        System.out.println("  3. 使用侧输出流处理异常数据");
        System.out.println("  4. 使用Timer处理超时");
    }

    // ==================== Full Outer Join ====================

    private static void fullOuterJoinExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- Full Outer Join 示例 ---");

        // 左流
        DataStream<Tuple2<String, Integer>> left = env.fromElements(
                Tuple2.of("A", 1),
                Tuple2.of("B", 2),
                Tuple2.of("C", 3)
        );

        // 右流
        DataStream<Tuple2<String, Integer>> right = env.fromElements(
                Tuple2.of("A", 10),
                Tuple2.of("B", 20),
                Tuple2.of("D", 40)  // 没有对应的左流数据
        );

        // Full Outer Join
        DataStream<String> fullOuterResult = left
                .fullOuterJoin(right)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply((leftVal, rightVal) -> {
                    if (leftVal != null && rightVal != null) {
                        return "BOTH: key=" + leftVal.f0 +
                               ", leftVal=" + leftVal.f1 +
                               ", rightVal=" + rightVal.f1;
                    } else if (leftVal != null) {
                        return "LEFT_ONLY: key=" + leftVal.f0 +
                               ", leftVal=" + leftVal.f1 +
                               ", rightVal=null";
                    } else {
                        return "RIGHT_ONLY: key=" + rightVal.f0 +
                               ", leftVal=null" +
                               ", rightVal=" + rightVal.f1;
                    }
                });

        System.out.println("Full Outer Join特点:");
        System.out.println("  - 左流和右流都可以有未匹配的数据");
        System.out.println("  - 未匹配的一侧值为null");
        System.out.println("  - 适用于需要保留所有数据的场景");
    }
}
