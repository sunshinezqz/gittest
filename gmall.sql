show databases;

show tables ;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

show functions;

set hive.exec.dynamic.partition.mode=nonstrict;

--建表语句
drop table if exists ods_base_dic;
create external table ods_base_dic(
    `dic_code` string COMMENT '编号',
    `dic_name` string  COMMENT '编码名称',
    `parent_code` string  COMMENT '父编码',
    `create_time` string  COMMENT '创建日期',
    `operate_time` string  COMMENT '操作日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_dic/';
--数据载入
--建表语句
create external table dwd_dim_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
tblproperties ("parquet.compression"="lzo");


--数据载入
insert overwrite table dwd_dim_activity_info partition (dt='2020-06-14')
select
    id,
    activity_name,
    activity_type,
    start_time,
    end_time,
    create_time
from ods_activity_info
where dt='2020-06-14';

--建表语句
CREATE EXTERNAL TABLE `dwd_dim_base_province` (
    `id` string COMMENT 'id',
    `province_name` string COMMENT '省市名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'ISO编码',
    `region_id` string COMMENT '地区id',
    `region_name` string COMMENT '地区名称'
) COMMENT '地区维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_base_province/'
tblproperties ("parquet.compression"="lzo");


--数据载入
insert overwrite table dwd_dim_base_province
select
    pro.id,
    pro.name,
    pro.area_code,
    pro.iso_code,
    pro.region_id,
    reg.region_name
from ods_base_province pro
join ods_base_region reg
on pro.region_id = reg.id;

--建表语句
CREATE EXTERNAL TABLE `dwd_dim_date_info`(
    `date_id` string COMMENT '日',
    `week_id` string COMMENT '周',
    `week_day` string COMMENT '星期几',
    `day` string COMMENT '每月的第几天',
    `month` string COMMENT '第几月',
    `quarter` string COMMENT '第几季度',
    `year` string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_date_info/'
tblproperties ("parquet.compression"="lzo");

CREATE EXTERNAL TABLE `dwd_dim_date_info_tmp`(
    `date_id` string COMMENT '日',
    `week_id` string COMMENT '周',
    `week_day` string COMMENT '星期几',
    `day` string COMMENT '每月的第几天',
    `month` string COMMENT '第几月',
    `quarter` string COMMENT '第几季度',
    `year` string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_date_info_tmp/';

--数据载入
insert overwrite table dwd_dim_date_info
select date_id, week_id, week_day, day, month, quarter, year, is_workday, holiday_id from dwd_dim_date_info_tmp;

--建表语句
create external table dwd_fact_payment_info (
    `id` string COMMENT 'id',
    `out_trade_no` string COMMENT '对外业务编号',
    `order_id` string COMMENT '订单编号',
    `user_id` string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `payment_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type` string COMMENT '支付类型',
    `payment_time` string COMMENT '支付时间',
    `province_id` string COMMENT '省份ID'
) COMMENT '支付事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_payment_info partition(dt='2020-06-14')
select
    pi.id,
    pi.out_trade_no,
    pi.order_id,
    pi.user_id,
    pi.alipay_trade_no,
    pi.total_amount,
    pi.subject,
    pi.payment_type,
    pi.payment_time,
    oi.province_id
from
(select * from ods_payment_info where dt='2020-06-14') pi
join
(select * from ods_order_info where dt='2020-06-14') oi
on pi.order_id = oi.id;



--建表语句
create external table dwd_fact_order_refund_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` bigint COMMENT '退款件数',
    `refund_amount` decimal(16,2) COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退款事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_order_refund_info partition (dt='2020-06-14')
select
    id,
    user_id,
    order_id,
    sku_id,
    refund_type,
    refund_num,
    refund_amount,
    refund_reason_type,
    create_time
from ods_order_refund_info
where dt='2020-06-14';

--建表语句
create external table dwd_fact_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_comment_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_comment_info partition (dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time
from ods_comment_info
where dt='2020-06-14';

--建表语句
create external table dwd_fact_order_detail (
    `id` string COMMENT '明细编号',
    `order_id` string COMMENT '订单号',
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT 'sku商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` decimal(16,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间',
    `province_id` string COMMENT '省份ID',
    `source_type` string COMMENT '来源类型',
    `source_id` string COMMENT '来源编号',
    `original_amount_d` decimal(20,2) COMMENT '原始价格分摊',
    `final_amount_d` decimal(20,2) COMMENT '购买价格分摊',
    `feight_fee_d` decimal(20,2) COMMENT '运费分摊',
    `benefit_reduce_amount_d` decimal(20,2) COMMENT '优惠分摊'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
tblproperties ("parquet.compression"="lzo");
--数据载入
select
    od.id,
    od.order_id,
    od.user_id,
    od.sku_id,
    od.sku_name,
    od.order_price,
    od.sku_num,
    od.create_time,
    oi.province_id,
    od.source_type,
    od.source_id,
    od.order_price * od.sku_num original_amount_d,
    od.order_price * od.sku_num / oi.original_total_amount * oi.final_total_amount final_amount_d,
    od.order_price * od.sku_num / oi.original_total_amount * oi.feight_fee feight_fee_d,
    od.order_price * od.sku_num / oi.original_total_amount * oi.benefit_reduce_amount benefit_reduce_amount_d
from
(select * from ods_order_detail where dt='2020-06-14') od
join
(select * from ods_order_info where dt='2020-06-14') oi
on od.order_id = oi.id;


insert overwrite table dwd_fact_order_detail partition (dt='2020-06-14')
select
    t1.id,
    t1.order_id,
    t1.user_id,
    t1.sku_id,
    t1.sku_name,
    t1.order_price,
    t1.sku_num,
    t1.create_time,
    t1.province_id,
    t1.source_type,
    t1.source_id,
    original_amount_d,
    if(rk=1,final_total_amount - final_total_amount_d_sum + final_amount_d,final_amount_d) final_amount_d,
    if(rk=1,feight_fee - feight_fee_d_sum + feight_fee_d,feight_fee_d) feight_fee_d,
    if(rk=1,benefit_reduce_amount - benefit_reduce_amount_d_sum + benefit_reduce_amount_d,benefit_reduce_amount_d) benefit_reduce_amount_d
from
(
    select
        od.id,
        od.order_id,
        od.user_id,
        od.sku_id,
        od.sku_name,
        od.order_price,
        od.sku_num,
        od.create_time,
        oi.province_id,
        od.source_type,
        od.source_id,
        round(od.order_price * od.sku_num,2) original_amount_d,
        round(od.order_price * od.sku_num / oi.original_total_amount * oi.final_total_amount,2) final_amount_d,
        round(od.order_price * od.sku_num / oi.original_total_amount * oi.feight_fee,2) feight_fee_d,
        round(od.order_price * od.sku_num / oi.original_total_amount * oi.benefit_reduce_amount,2) benefit_reduce_amount_d,
        oi.final_total_amount,
        oi.feight_fee,
        oi.benefit_reduce_amount,
        sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.final_total_amount,2)) over(partition by od.order_id) final_total_amount_d_sum,
        sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.feight_fee,2)) over(partition by od.order_id) feight_fee_d_sum,
        sum(round(od.order_price * od.sku_num / oi.original_total_amount * oi.benefit_reduce_amount,2)) over(partition by od.order_id) benefit_reduce_amount_d_sum,
        rank() over (partition by od.order_id order by od.id) rk
    from
    (select * from ods_order_detail where dt='2020-06-14') od
    join
    (select * from ods_order_info where dt='2020-06-14') oi
    on od.order_id = oi.id
) t1;

--建表语句
create external table dwd_fact_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` string  COMMENT '放入购物车时价格',
    `sku_num` string  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单。1为已下单;0为未下单',
    `order_time` string  COMMENT '下单时间',
    `source_type` string COMMENT '来源类型',
    `srouce_id` string COMMENT '来源编号'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_cart_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_cart_info partition (dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    cart_price,
    sku_num,
    sku_name,
    create_time,
    operate_time,
    is_ordered,
    order_time,
    source_type,
    source_id
from ods_cart_info
where dt='2020-06-14';


--建表语句
create external table dwd_fact_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_favor_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_favor_info partition (dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ods_favor_info
where dt='2020-06-14';

--建表语句
create external table dwd_fact_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'userid',
    `order_id` string  COMMENT '订单id',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_coupon_use/'
tblproperties ("parquet.compression"="lzo");

--数据载入
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

insert overwrite table dwd_fact_coupon_use partition (dt)
select
    nvl(new.id,old.id),
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.coupon_status,old.coupon_status),
    nvl(new.get_time,old.get_time),
    nvl(new.using_time,old.using_time),
    nvl(new.used_time,old.used_time),
    date_format(nvl(new.get_time,old.get_time),'yyyy-MM-dd')
from
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from dwd_fact_coupon_use
    where dt in
    (
        select
            date_format(ods_coupon_use.get_time,'yyyy-MM-dd')
        from ods_coupon_use
        where ods_coupon_use.dt='2020-06-14'
    )
) old
full join
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from ods_coupon_use
    where dt='2020-06-14'
) new
on old.id = new.id;

--建表语句
create external table dwd_fact_order_info (
    `id` string COMMENT '订单编号',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间(未支付状态)',
    `payment_time` string COMMENT '支付时间(已支付状态)',
    `cancel_time` string COMMENT '取消时间(已取消状态)',
    `finish_time` string COMMENT '完成时间(已完成状态)',
    `refund_time` string COMMENT '退款时间(退款中状态)',
    `refund_finish_time` string COMMENT '退款完成时间(退款完成状态)',
    `province_id` string COMMENT '省份ID',
    `activity_id` string COMMENT '活动ID',
    `original_total_amount` decimal(16,2) COMMENT '原价金额',
    `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
    `feight_fee` decimal(16,2) COMMENT '运费',
    `final_total_amount` decimal(16,2) COMMENT '订单金额'
) COMMENT '订单事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_info/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwd_fact_order_info partition (dt)
select
    if(new.id is null,old.id,new.id),
    if(new.order_status is null,old.order_status,new.order_status),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.out_trade_no is null,old.out_trade_no,new.out_trade_no),
    if(new.tms['1001'] is null,old.create_time,new.tms['1001']),
    if(new.tms['1002'] is null,old.payment_time,new.tms['1002']),
    if(new.tms['1003'] is null,old.cancel_time,new.tms['1003']),
    if(new.tms['1004'] is null,old.finish_time,new.tms['1004']),
    if(new.tms['1005'] is null,old.refund_time,new.tms['1005']),
    if(new.tms['1006'] is null,old.refund_finish_time,new.tms['1006']),
    if(new.province_id is null,old.province_id,new.province_id),
    if(new.activity_id is null,old.activity_id,new.activity_id),
    if(new.original_total_amount is null,old.original_total_amount,new.original_total_amount),
    if(new.benefit_reduce_amount is null,old.benefit_reduce_amount,new.benefit_reduce_amount),
    if(new.feight_fee is null,old.feight_fee,new.feight_fee),
    if(new.final_total_amount is null,old.final_total_amount,new.final_total_amount),
    date_format(if(new.tms['1001'] is null,old.create_time,new.tms['1001']),'yyyy-MM-dd')
from
(
    select
        id,
        order_status,
        user_id,
        out_trade_no,
        create_time,
        payment_time,
        cancel_time,
        finish_time,
        refund_time,
        refund_finish_time,
        province_id,
        activity_id,
        original_total_amount,
        benefit_reduce_amount,
        feight_fee,
        final_total_amount
    from dwd_fact_order_info
    where dt in
    (
        select
            date_format(ods_order_info.create_time,'yyyy-MM-dd')
        from ods_order_info
        where dt='2020-06-14'
    )
) old
full join
(
    select
        info.id,
        info.order_status,
        info.user_id,
        info.out_trade_no,
        info.province_id,
        act.activity_id,
        log.tms,
        info.original_total_amount,
        info.benefit_reduce_amount,
        info.feight_fee,
        info.final_total_amount
    from
    (
        select
            id,
            final_total_amount,
            order_status,
            user_id,
            out_trade_no,
            create_time,
            operate_time,
            province_id,
            benefit_reduce_amount,
            original_total_amount,
            feight_fee
        from ods_order_info
        where dt='2020-06-14'
    ) info
    join
    (
        select
            order_id,
            str_to_map(concat_ws(',',collect_list(concat(order_status,'=',operate_time))),',','=') tms
        from ods_order_status_log
        where dt='2020-06-14'
        group by order_id
    ) log
    on info.id = log.order_id
    left join
    (
        select
            order_id,
            activity_id
        from ods_activity_order
        where dt='2020-06-14'
    ) act
    on info.id = act.order_id
) new
on old.id = new.id;


--建表语句
create external table dwd_dim_user_info_his(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
tblproperties ("parquet.compression"="lzo");

--数据载入
--1.初始化拉链表
insert overwrite table dwd_dim_user_info_his
select
    id,
    name,
    birthday,
    gender,
    email,
    user_level,
    create_time,
    operate_time,
    '2020-06-14',
    '9999-99-99'
from ods_user_info
where dt='2020-06-14';

--2.获取当天的用户变化表
select
    *
from ods_user_info
where dt='2020-06-15';

--3.创建临时拉链表
create external table dwd_dim_user_info_his_tmp(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his_tmp/'
tblproperties ("parquet.compression"="lzo");

--4.拉链
insert overwrite table dwd_dim_user_info_his_tmp
select
    t1.id,
    t1.name,
    t1.birthday,
    t1.gender,
    t1.email,
    t1.user_level,
    t1.create_time,
    t1.operate_time,
    t1.start_date,
    t1.end_date
from
(
    select
        old.id,
        old.name,
        old.birthday,
        old.gender,
        old.email,
        old.user_level,
        old.create_time,
        old.operate_time,
        old.start_date,
        if(new.id is not null and old.end_date = '9999-99-99',date_add(new.dt,-1),old.end_date) end_date
    from (select * from dwd_dim_user_info_his where start_date < '2020-06-15') old
    left join
    (
        select
            *
        from ods_user_info
        where dt='2020-06-15'
    ) new
    on old.id = new.id
    union all
    select
        id,
        name,
        birthday,
        gender,
        email,
        user_level,
        create_time,
        operate_time,
        '2020-06-15' start_date,
        '9999-99-99' end_date
    from ods_user_info
    where dt='2020-06-15'
) t1
order by cast(id as bigint),start_date;

--5.将临时表的数据覆写到拉链表中
insert overwrite table dwd_dim_user_info_his
select * from dwd_dim_user_info_his_tmp;
--建表语句
create external table dws_uv_detail_daycount
(
    `mid_id`      string COMMENT '设备id',
    `brand`       string COMMENT '手机品牌',
    `model`       string COMMENT '手机型号',
    `login_count` bigint COMMENT '活跃次数',
    `page_stats`  array<struct<page_id:string,page_count:bigint>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dws_uv_detail_daycount partition (dt='2020-06-14')
select
    login.mid_id,
    login.brand,
    login.model,
    login.login_count,
    page.page_stats
from
(
    select
        mid_id,
        brand,
        model,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    group by mid_id,brand,model
) login
left join
(
    select
        mid_id,
        brand,
        model,
        collect_list(named_struct('page_id',page_id,'page_count',page_count)) page_stats
    from
    (
        select
            mid_id,
            brand,
            model,
            page_id,
            count(*) page_count
        from dwd_page_log
        where dt='2020-06-14'
        group by mid_id,brand,model,page_id
    ) t1
    group by mid_id,brand,model
) page
on login.mid_id = page.mid_id
and login.brand = page.brand
and login.model = page.model;

--建表语句
create external table dwt_uv_topic
(
    `mid_id` string comment '设备id',
    `brand` string comment '手机品牌',
    `model` string comment '手机型号',
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwt_uv_topic
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.brand,old.brand),
    nvl(new.model,old.model),
    if(old.mid_id is null,'2020-06-14',old.login_date_first),
    if(new.mid_id is not null,'2020-06-14',old.login_date_last),
    if(new.login_count is not null,new.login_count,0),
    nvl(old.login_count,0) + if(new.login_count > 0,1,0)
from
    dwt_uv_topic old
full join
(
    select
        *
    from dws_uv_detail_daycount
    where dt='2020-06-14'
) new
on old.mid_id = new.mid_id
and old.brand = new.brand
and old.model = new.model;

--建表语句
create external table dws_user_action_daycount
(
    user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    cart_count bigint comment '加入购物车次数',
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额',
    order_detail_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");


--数据载入
with
tmp_start as
(
    select
        user_id,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id = 'cart_add'
    group by user_id
),
tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from dwd_fact_order_info
    where dt='2020-06-14'
    group by user_id
),
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_fact_payment_info
    where dt='2020-06-14'
    group by user_id
),
tmp_order_detail as
(
    select
        user_id,
        collect_list(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'order_amount',order_amount))  order_detail_stats
    from
    (
        select
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(final_amount_d) as decimal(20,2)) order_amount
        from dwd_fact_order_detail
        where dt='2020-06-14'
        group by user_id,sku_id
    ) t1
    group by user_id
)
insert overwrite table dws_user_action_daycount partition (dt='2020-06-14')
select
    tmp_start.user_id,
    login_count,
    nvl(cart_count,0),
    nvl(order_count,0),
    nvl(order_amount,0.0),
    nvl(payment_count,0),
    nvl(payment_amount,0.0),
    order_detail_stats
from tmp_start
left join tmp_cart on tmp_start.user_id = tmp_cart.user_id
left join tmp_order on tmp_start.user_id = tmp_order.user_id
left join tmp_payment on tmp_start.user_id = tmp_payment.user_id
left join tmp_order_detail on tmp_start.user_id = tmp_order_detail.user_id;


--建表语句
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
)COMMENT '会员主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.user_id is null and new.login_count > 0,'2020-06-14',old.login_date_first),
    if(new.login_count > 0,'2020-06-14',old.login_date_last),
    nvl(old.login_count,0) + if(new.login_count > 0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.user_id is null and new.order_count > 0,'2020-06-14',old.order_date_first),
    if(new.order_count > 0,'2020-06-14',old.order_date_last),
    nvl(old.order_count,0) + nvl(new.order_count,0),
    nvl(old.order_amount,0) + nvl(new.order_amount,0),
    nvl(new.order_30d_count,0),
    nvl(new.order_30d_amount,0),
    if(old.user_id is null and new.payment_count > 0,'2020-06-14',old.payment_date_first),
    if(new.payment_count > 0,'2020-06-14',old.payment_date_last),
    nvl(old.payment_count,0) + nvl(new.payment_count,0),
    nvl(old.payment_amount,0) + nvl(new.payment_amount,0),
    nvl(new.payment_30d_count,0),
    nvl(new.payment_30d_amount,0)
from dwt_user_topic old
full join
(
    select
        user_id,
        sum(if(dt='2020-06-14',login_count,0)) login_count, --2020-06-14的登录次数
        sum(if(login_count > 0,1,0)) login_last_30d_count,
        sum(if(dt='2020-06-14',order_count,0)) order_count,--2020-06-14的下单次数
        sum(if(dt='2020-06-14',order_amount,0)) order_amount,--2020-06-14的下单金额
        sum(order_count) order_30d_count, --最近30日下单次数
        sum(order_amount) order_30d_amount, --最近30日下单金额
        sum(if(dt='2020-06-14',payment_count,0)) payment_count,--2020-06-14的支付次数
        sum(if(dt='2020-06-14',payment_amount,0)) payment_amount,--2020-06-14的支付金额
        sum(payment_count) payment_30d_count, --最近30日支付次数
        sum(payment_amount) payment_30d_amount --最近30日支付金额
    from dws_user_action_daycount
    where dt >= date_add('2020-06-14',-30) and dt <= '2020-06-14'
    group by user_id
) new
on old.user_id = new.user_id;

--建表语句
create external table dws_sku_action_daycount
(
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

--数据载入
with
tmp_order as
(
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(final_amount_d) order_amount
    from dwd_fact_order_detail
    where dt='2020-06-14'
    group by sku_id
),
tmp_payment as
(
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(final_amount_d) payment_amount
    from dwd_fact_order_detail
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    and order_id in
    (
        select
            order_id
        from dwd_fact_payment_info
        where dt='2020-06-14'
    )
    group by sku_id
),
tmp_refund as
(
    select
        sku_id,
        count(*) refund_count,
        sum(refund_num) refund_num,
        sum(refund_amount) refund_amount
    from dwd_fact_order_refund_info
    where dt='2020-06-14'
    group by sku_id
),
tmp_cart as
(
    select
        item sku_id,
        count(*) cart_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='cart_add'
    group by item
),
tmp_favor as
(
    select
        item sku_id,
        count(*) favor_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='favor_add'
    group by item
),
tmp_appraise as
(
    select
        sku_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from dwd_fact_comment_info
    where dt='2020-06-14'
    group by sku_id
)
insert overwrite table dws_sku_action_daycount partition (dt='2020-06-14')
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_count),
    sum(refund_num),
    sum(refund_amount),
    sum(cart_count),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count)
from
(
    select
        sku_id,
        order_count,
        order_num,
        order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_payment
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_refund
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cart
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_favor
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_appraise
) tmp
group by sku_id;

--建表语句
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(16,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(16,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_count bigint comment '累积被加入购物车次数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");

--数据载入
insert overwrite table dwt_sku_topic
select
    nvl(new.sku_id,old.sku_id),
    sku_info.spu_id,
    nvl(new.order_count30,0),
    nvl(new.order_num30,0),
    nvl(new.order_amount30,0),
    nvl(old.order_count,0) + nvl(new.order_count,0),
    nvl(old.order_num,0) + nvl(new.order_num,0),
    nvl(old.order_amount,0) + nvl(new.order_amount,0),
    nvl(new.payment_count30,0),
    nvl(new.payment_num30,0),
    nvl(new.payment_amount30,0),
    nvl(old.payment_count,0) + nvl(new.payment_count,0),
    nvl(old.payment_num,0) + nvl(new.payment_num,0),
    nvl(old.payment_amount,0) + nvl(new.payment_amount,0),
    nvl(new.refund_count30,0),
    nvl(new.refund_num30,0),
    nvl(new.refund_amount30,0),
    nvl(old.refund_count,0) + nvl(new.refund_count,0),
    nvl(old.refund_num,0) + nvl(new.refund_num,0),
    nvl(old.refund_amount,0) + nvl(new.refund_amount,0),
    nvl(new.cart_count30,0),
    nvl(old.cart_count,0) + nvl(new.cart_count,0),
    nvl(new.favor_count30,0),
    nvl(old.favor_count,0) + nvl(new.favor_count,0),
    nvl(new.appraise_good_count30,0),
    nvl(new.appraise_mid_count30,0),
    nvl(new.appraise_bad_count30,0),
    nvl(new.appraise_default_count30,0)  ,
    nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0),
    nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0),
    nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0)
from
dwt_sku_topic old
full outer join
(
    select
        sku_id,
        sum(if(dt='2020-06-14',order_count,0 )) order_count,
        sum(if(dt='2020-06-14',order_num ,0 ))  order_num,
        sum(if(dt='2020-06-14',order_amount,0 )) order_amount ,
        sum(if(dt='2020-06-14',payment_count,0 )) payment_count,
        sum(if(dt='2020-06-14',payment_num,0 )) payment_num,
        sum(if(dt='2020-06-14',payment_amount,0 )) payment_amount,
        sum(if(dt='2020-06-14',refund_count,0 )) refund_count,
        sum(if(dt='2020-06-14',refund_num,0 )) refund_num,
        sum(if(dt='2020-06-14',refund_amount,0 )) refund_amount,
        sum(if(dt='2020-06-14',cart_count,0 )) cart_count,
        sum(if(dt='2020-06-14',favor_count,0 )) favor_count,
        sum(if(dt='2020-06-14',appraise_good_count,0 )) appraise_good_count,
        sum(if(dt='2020-06-14',appraise_mid_count,0 ) ) appraise_mid_count ,
        sum(if(dt='2020-06-14',appraise_bad_count,0 )) appraise_bad_count,
        sum(if(dt='2020-06-14',appraise_default_count,0 )) appraise_default_count,
        sum(order_count) order_count30 ,
        sum(order_num) order_num30,
        sum(order_amount) order_amount30,
        sum(payment_count) payment_count30,
        sum(payment_num) payment_num30,
        sum(payment_amount) payment_amount30,
        sum(refund_count) refund_count30,
        sum(refund_num) refund_num30,
        sum(refund_amount) refund_amount30,
        sum(cart_count) cart_count30,
        sum(favor_count) favor_count30,
        sum(appraise_good_count) appraise_good_count30,
        sum(appraise_mid_count) appraise_mid_count30,
        sum(appraise_bad_count) appraise_bad_count30,
        sum(appraise_default_count) appraise_default_count30
    from dws_sku_action_daycount
    where dt >= date_add ('2020-06-14', -30)
    group by sku_id
)new
on new.sku_id = old.sku_id
left join
(select id,spu_id from dwd_dim_sku_info where dt='2020-06-14') sku_info
on nvl(new.sku_id,old.sku_id)= sku_info.id;

--建表语句
create external table dws_activity_info_daycount(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_count` bigint COMMENT '曝光次数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

--数据载入
with
tmp_activity as
(
    select
        id,
        activity_name,
        activity_type,
        start_time,
        end_time,
        create_time
    from dwd_dim_activity_info
    where dt='2020-06-14'
),
tmp_display as
(
    select
        item activity_id,
        count(*) display_count
    from dwd_display_log
    where dt='2020-06-14'
    and display_type='activity'
    group by item
),
tmp_op as
(
    select
        activity_id,
        sum(if(date_format(create_time,'yyyy-MM-dd') = '2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd') = '2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd') = '2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd') = '2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    and activity_id is not null
    group by activity_id
)
insert overwrite table dws_activity_info_daycount partition(dt='2020-06-14')
select
    nvl(tmp_op.activity_id,tmp_display.activity_id),
    tmp_activity.activity_name,
    tmp_activity.activity_type,
    tmp_activity.start_time,
    tmp_activity.end_time,
    tmp_activity.create_time,
    tmp_display.display_count,
    tmp_op.order_count,
    tmp_op.order_amount,
    tmp_op.payment_count,
    tmp_op.payment_amount
from tmp_op
full join tmp_display on tmp_op.activity_id = tmp_display.activity_id
left join tmp_activity on nvl(tmp_op.activity_id,tmp_display.activity_id) = tmp_activity.id;



--建表语句
create external table dwt_activity_topic(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_day_count` bigint COMMENT '当日曝光次数',
    `order_day_count` bigint COMMENT '当日下单次数',
    `order_day_amount` decimal(20,2) COMMENT '当日下单金额',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20,2) COMMENT '当日支付金额',
    `display_count` bigint COMMENT '累积曝光次数',
    `order_count` bigint COMMENT '累积下单次数',
    `order_amount` decimal(20,2) COMMENT '累积下单金额',
    `payment_count` bigint COMMENT '累积支付次数',
    `payment_amount` decimal(20,2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");


--数据载入
insert overwrite table dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.activity_type,old.activity_type),
    nvl(new.start_time,old.start_time),
    nvl(new.end_time,old.end_time),
    nvl(new.create_time,old.create_time),
    nvl(new.display_count,0),
    nvl(new.order_count,0),
    nvl(new.order_amount,0),
    nvl(new.payment_count,0),
    nvl(new.payment_amount,0),
    nvl(new.display_count,0) + nvl(old.display_count,0),
    nvl(new.order_count,0) + nvl(old.order_count,0),
    nvl(new.order_amount,0) + nvl(old.order_amount,0),
    nvl(new.payment_count,0) + nvl(old.payment_count,0),
    nvl(new.payment_amount,0) + nvl(old.payment_amount,0)
from dwt_activity_topic old
full join
(
    select
        id,
        activity_name,
        activity_type,
        start_time,
        end_time,
        create_time,
        display_count,
        order_count,
        order_amount,
        payment_count,
        payment_amount
    from dws_activity_info_daycount
    where dt='2020-06-14'
) new
on old.id = new.id;

--建表语句
create external table dws_area_stats_daycount(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_count` string COMMENT '活跃设备数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_area_stats_daycount/'
tblproperties ("parquet.compression"="lzo");

--数据载入
with
tmp_login as
(
    select
        area_code,
        count(*) login_count
    from
    (
        select
            area_code,
            mid_id,
            brand,
            model
        from dwd_start_log
        where dt='2020-06-14'
        group by area_code,mid_id,brand,model
    ) t1
    group by area_code
),
tmp_op as
(
    select
        province_id,
        sum(if(date_format(create_time,'yyyy-MM-dd') = '2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd') = '2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd') = '2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd') = '2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where dt='2020-06-14' or dt=date_add('2020-06-14',-1)
    group by province_id
)
insert overwrite table dws_area_stats_daycount partition (dt='2020-06-14')
select
    pro.id,
    pro.province_name,
    pro.area_code,
    pro.iso_code,
    pro.region_id,
    pro.region_name,
    nvl(tmp_login.login_count,0),
    nvl(tmp_op.order_count,0),
    nvl(tmp_op.order_amount,0.0),
    nvl(tmp_op.payment_count,0),
    nvl(tmp_op.payment_amount,0.0)
from dwd_dim_base_province pro
left join tmp_login on pro.area_code = tmp_login.area_code
left join tmp_op on pro.id = tmp_op.province_id;

--建表语句
create external table dwt_area_topic(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` string COMMENT '当天活跃设备数',
    `login_last_30d_count` string COMMENT '最近30天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `order_last_30d_count` bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount` decimal(16,2) COMMENT '最近30天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额',
    `payment_last_30d_count` bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16,2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_area_topic/'
tblproperties ("parquet.compression"="lzo");


--数据载入
insert overwrite table dwt_area_topic
select
    nvl(new.id,old.id),
    nvl(new.province_name,old.province_name),
    nvl(new.area_code,old.area_code),
    nvl(new.iso_code,old.iso_code),
    nvl(new.region_id,old.region_id),
    nvl(new.region_name,old.region_name),
    nvl(new.login_count,0),
    nvl(new.login_30d_count,0),
    nvl(new.order_count,0),
    nvl(new.order_amount,0),
    nvl(new.order_30d_count,0),
    nvl(new.order_30d_amount,0),
    nvl(new.payment_count,0),
    nvl(new.payment_amount,0),
    nvl(new.payment_30d_count,0),
    nvl(new.payment_30d_amount,0)
from dwt_area_topic old
full join
(
select
    id,
    province_name,
    area_code,
    iso_code,
    region_id,
    region_name,
    sum(if(dt='2020-06-14',login_count,0)) login_count,
    sum(login_count) login_30d_count,
    sum(if(dt='2020-06-14',order_count,0)) order_count,
    sum(if(dt='2020-06-14',order_amount,0)) order_amount,
    sum(order_count) order_30d_count,
    sum(order_amount) order_30d_amount,
    sum(if(dt='2020-06-14',payment_count,0)) payment_count,
    sum(if(dt='2020-06-14',payment_amount,0)) payment_amount,
    sum(payment_count) payment_30d_count,
    sum(payment_amount) payment_30d_amount
from dws_area_stats_daycount
where dt>=date_add('2020-06-14',-30)
group by id,province_name,area_code,iso_code,region_id,region_name
) new
on old.id = new.id;

--建表语句
create external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';

--数据载入
insert into table ads_uv_count
select
    '2020-06-25',
    day_count.day_count,
    wk_count.wk_count,
    mn_count.mn_count,
    if(date_add(next_day('2020-06-25','mo'),-1) = '2020-06-25','Y','N'),
    if(last_day('2020-06-25') = '2020-06-25','Y','N')
from
(
    select
        '2020-06-25' dd,
        count(*) day_count
    from dwt_uv_topic
    where login_date_last ='2020-06-25'
) day_count
join
(
    select
        '2020-06-25' dd,
        count(*) wk_count
    from dwt_uv_topic
    where login_date_last >= date_add(next_day('2020-06-25','mo'),-7)
    and login_date_last <= date_add(next_day('2020-06-25','mo'),-1)
) wk_count
on day_count.dd = wk_count.dd
join
(
    select
        '2020-06-25' dd,
        count(*) mn_count
    from dwt_uv_topic
    where date_format(login_date_last,'yyyy-MM') = date_format('2020-06-25','yyyy-MM')
) mn_count
on day_count.dd = mn_count.dd;

--建表语句
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

--数据载入
insert into table ads_new_mid_count
select
    '2020-06-25',
    count(*)
from dwt_uv_topic
where login_date_first ='2020-06-25'

--建表语句
create external table ads_silent_count(
    `dt` string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';

--数据载入
insert into table ads_silent_count
select
   '2020-06-25',
   count(*)
from dwt_uv_topic
where login_date_first = login_date_last
and login_date_first <= date_add('2020-06-25',-7);

--建表语句
create external table ads_wastage_count(
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) COMMENT '流失用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';

--数据载入
insert into table ads_wastage_count
select
   '2020-06-25',
   count(*)
from dwt_uv_topic
where login_date_last <= date_add('2020-06-25',-7);

--建表语句
create external table ads_back_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

--数据载入
insert into table ads_back_count
select
    '2020-06-25',
    concat(date_add(next_day('2020-06-25','mo'),-7),'_',date_add(next_day('2020-06-25','mo'),-1)),
    count(*)
from
(
    select
        mid_id
    from dwt_uv_topic
    where login_date_last >= date_add(next_day('2020-06-25','mo'),-7)
    and login_date_last <= date_add(next_day('2020-06-25','mo'),-1)
    and login_date_first < date_add(next_day('2020-06-25','mo'),-7)
) wk
left join
(
    select
        mid_id
    from dws_uv_detail_daycount
    where dt >= date_add(next_day('2020-06-25','mo'),-7*2)
    and dt <= date_add(next_day('2020-06-25','mo'),-1-7)
    group by mid_id
) last_wk
on wk.mid_id = last_wk.mid_id
where last_wk.mid_id is null;

--建表语句
create external table ads_continuity_wk_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '活跃设备数'
) COMMENT '最近连续三周活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

--数据载入
insert into table ads_continuity_wk_count
select
    '2020-06-25',
    concat(date_add(next_day('2020-06-25','mo'),-7*3),'_',date_add(next_day('2020-06-25','mo'),-1)),
    count(*)
from
(
    select
        mid_id
    from
    (
        select
            mid_id
        from dws_uv_detail_daycount
        where dt >= date_add(next_day('2020-06-25','mo'),-7)
        and dt <= date_add(next_day('2020-06-25','mo'),-1)
        group by mid_id
        union all
        select
            mid_id
        from dws_uv_detail_daycount
        where dt >= date_add(next_day('2020-06-25','mo'),-7*2)
        and dt <= date_add(next_day('2020-06-25','mo'),-1-7)
        group by mid_id
        union all
        select
            mid_id
        from dws_uv_detail_daycount
        where dt >= date_add(next_day('2020-06-25','mo'),-7*3)
        and dt <= date_add(next_day('2020-06-25','mo'),-1-7*2)
        group by mid_id
    ) t1
    group by mid_id
    having count(*) = 3
) t2;

--建表语句
create external table ads_continuity_uv_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '最近七天内连续三天活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';

--数据载入
insert into table ads_continuity_uv_count
select
    '2020-06-25',
    concat(date_add('2020-06-25',-6),'_','2020-06-25'),
    count(*)
from
(
    select
        mid_id
    from
    (
        select
            mid_id
        from
        (
            select
                mid_id,
                dt,
                rk,
                date_add(dt,-rk) date_diff
            from
            (
                select
                    mid_id,
                    dt,
                    rank() over (partition by mid_id order by dt) rk
                from
                (
                    select
                        mid_id,
                        dt
                    from dws_uv_detail_daycount
                    where dt >= date_add('2020-06-25',-6)
                    group by mid_id,dt
                ) t1
            ) t2
        ) t3
        group by mid_id,date_diff
        having count(*) >= 3
    ) t4
    group by mid_id
) t5;

--建表语句
create external table ads_user_retention_day_rate
(
     `stat_date`          string comment '统计日期',
     `create_date`       string  comment '设备新增日期',
     `retention_day`     int comment '截止当前日期留存天数',
     `retention_count`    bigint comment  '留存数量',
     `new_mid_count`     bigint comment '设备新增数量',
     `retention_ratio`   decimal(16,2) comment '留存率'
)  COMMENT '留存率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

--数据载入
insert into table ads_user_retention_day_rate
select
    '2020-06-25',
    date_add('2020-06-25',-1),
    1,
    sum(if(login_date_first=date_add('2020-06-25',-1) and login_date_last = '2020-06-25',1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-1),1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-1) and login_date_last = '2020-06-25',1,0)) / sum(if(login_date_first=date_add('2020-06-25',-1),1,0)) * 100
from dwt_uv_topic
union all

--2. 计算2020-06-23的2日留存率 = 2020-06-23的2日留存用户数 / 2020-06-23的新增用户数
select
    '2020-06-25',
    date_add('2020-06-25',-2),
    2,
    sum(if(login_date_first=date_add('2020-06-25',-2) and login_date_last = '2020-06-25',1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-2),1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-2) and login_date_last = '2020-06-25',1,0)) / sum(if(login_date_first=date_add('2020-06-25',-2),1,0)) * 100
from dwt_uv_topic
union all
--3. 计算2020-06-22的3日留存率 = 2020-06-22的3日留存用户数 / 2020-06-22的新增用户数
select
    '2020-06-25',
    date_add('2020-06-25',-3),
    3,
    sum(if(login_date_first=date_add('2020-06-25',-3) and login_date_last = '2020-06-25',1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-3),1,0)),
    sum(if(login_date_first=date_add('2020-06-25',-3) and login_date_last = '2020-06-25',1,0)) / sum(if(login_date_first=date_add('2020-06-25',-3),1,0)) * 100
from dwt_uv_topic;

--建表语句
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(16,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(16,2) COMMENT '会员付费率',
    `day_new_users2users` decimal(16,2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_topic';

--数据载入
insert into table ads_user_topic
select
    '2020-06-25',
    sum(if(login_date_last='2020-06-25',1,0)),
    sum(if(login_date_first='2020-06-25',1,0)),
    sum(if(payment_date_first='2020-06-25',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    sum(if(login_date_last='2020-06-25',1,0))/count(*) * 100,
    sum(if(payment_count>0,1,0))/count(*) * 100,
    sum(if(login_date_first='2020-06-25',1,0))/sum(if(login_date_last='2020-06-25',1,0)) * 100
from dwt_user_topic;

--建表语句
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `home_count`  bigint COMMENT '浏览首页人数',
    `good_detail_count` bigint COMMENT '浏览商品详情页人数',
    `home2good_detail_convert_ratio` decimal(16,2) COMMENT '首页到商品详情转化率',
    `cart_count` bigint COMMENT '加入购物车的人数',
    `good_detail2cart_convert_ratio` decimal(16,2) COMMENT '商品详情页到加入购物车转化率',
    `order_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(16,2) COMMENT '加入购物车到下单转化率',
    `payment_count` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(16,2) COMMENT '下单到支付的转化率'
) COMMENT '漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';


--数据载入
insert into table ads_user_action_convert_day
select
    '2020-06-25',
    tmp_page.home_count,
    tmp_page.good_detail_count,
    nvl(tmp_page.good_detail_count / tmp_page.home_count * 100,0),
    tmp_cop.cart_count,
    nvl(tmp_cop.cart_count / tmp_page.good_detail_count * 100,0),
    tmp_cop.order_count,
    nvl(tmp_cop.order_count / tmp_cop.cart_count * 100,0),
    tmp_cop.payment_count,
    nvl(tmp_cop.payment_count / tmp_cop.order_count * 100,0)
from
(
    select
        '2020-06-25' dd,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from
    (
        select
            mid_id,
            page_id
        from dwd_page_log
        where dt='2020-06-25'
        and page_id in ('home','good_detail')
        group by mid_id,page_id
    ) t1
) tmp_page
join
(
    select
        '2020-06-25' dd,
        sum(if(cart_count > 0,1,0)) cart_count,
        sum(if(order_count > 0,1,0)) order_count,
        sum(if(payment_count > 0,1,0)) payment_count
    from dws_user_action_daycount
    where dt='2020-06-25'
) tmp_cop
on tmp_page.dd = tmp_cop.dd;

--建表语句
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';

--数据载入
select
    '2020-06-25',
    count(sku_id),
    count(distinct(spu_id))
from dwt_sku_topic;

insert into table ads_product_info
select
    '2020-06-25',
    sku.sku_num,
    spu.spu_num
from
(
    select
        '2020-06-25' dd,
        count(sku_id) sku_num
    from dwt_sku_topic
) sku
join
(
    select
        '2020-06-25' dd,
        count(spu_id) spu_num
    from
    (
        select
            spu_id
        from dwt_sku_topic
        group by spu_id
    ) t1
) spu
on sku.dd = spu.dd;

--建表语句
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量金额'
) COMMENT '商品销量排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';

--数据载入
insert into table ads_product_sale_topN
select
    '2020-06-25',
    sku_id,
    payment_amount
from dws_sku_action_daycount
where dt='2020-06-25'
order by payment_amount desc
limit 10;


--建表语句
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';

--数据载入
insert into table ads_product_favor_topN
select
    '2020-06-25',
    sku_id,
    favor_count
from dws_sku_action_daycount
where dt='2020-06-25'
order by favor_count desc
limit 10;

--建表语句
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_count` bigint COMMENT '加入购物车次数'
) COMMENT '商品加入购物车排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';

--数据载入
insert into table ads_product_cart_topN
select
    '2020-06-25',
    sku_id,
    cart_count
from dws_sku_action_daycount
where dt='2020-06-25'
order by cart_count desc
limit 10;

--建表语句
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(16,2) COMMENT '退款率'
) COMMENT '商品退款率排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';

--数据载入
insert into table ads_product_refund_topN
select
    '2020-06-25',
    sku_id,
    refund_last_30d_count / payment_last_30d_count * 100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc
limit 10;

--建表语句
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16,2) COMMENT '差评率'
) COMMENT '商品差评率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';

--数据载入
insert into table ads_appraise_bad_topN
select
    '2020-06-25',
    sku_id,
    appraise_bad_count / (appraise_good_count + appraise_mid_count + appraise_bad_count + appraise_default_count) * 100 appraise_bad_ratio
from dws_sku_action_daycount
where dt='2020-06-25'
order by appraise_bad_ratio desc
limit 10;

--建表语句
create external table ads_order_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users bigint comment '单日下单会员数'
) comment '下单数目统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_order_daycount';

--数据载入
insert into table ads_order_daycount
select
    '2020-06-25',
    sum(order_count),
    sum(order_amount),
    sum(if(order_count >0,1,0))
from dws_user_action_daycount
where dt='2020-06-25';

--建表语句
create external table ads_payment_daycount(
    dt string comment '统计日期',
    payment_count bigint comment '单日支付笔数',
    payment_amount bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count bigint comment '单日支付商品sku数',
    payment_avg_time decimal(16,2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_payment_daycount';

--数据载入
with
tmp_payment as
(
    select
        '2020-06-25' dd,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(payment_count >0,1,0)) payment_user_count
    from dws_user_action_daycount
    where dt='2020-06-25'
),
tmp_sku as
(
    select
        '2020-06-25' dd,
        sum(if(payment_count>0,1,0)) payment_sku_count
    from dws_sku_action_daycount
    where dt='2020-06-25'
),
tmp_time as
(
    select
        '2020-06-25' dd,
        avg((unix_timestamp(payment_time) - unix_timestamp(create_time)) / 60) avg_time
    from dwd_fact_order_info
    where dt='2020-06-25'
    and payment_time is not null
)
insert into table ads_payment_daycount
select
    '2020-06-25',
    tmp_payment.payment_count,
    tmp_payment.payment_amount,
    tmp_payment.payment_user_count,
    tmp_sku.payment_sku_count,
    tmp_time.avg_time
from tmp_payment
join tmp_sku on tmp_payment.dd = tmp_sku.dd
join tmp_time on tmp_payment.dd = tmp_time.dd;

--建表语句
create external table ads_sale_tm_category1_stat_mn
(
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(16,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(16,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期'
) COMMENT '品牌复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';


--数据载入
with
tmp_order as
(
    select
        user_id,
        sku_id,
        count(*) user_sku_count
    from dwd_fact_order_detail
    where date_format(dt,'yyyy-MM') = date_format('2020-06-25','yyyy-MM')
    group by user_id,sku_id
),
tmp_sku as
(
    select
        id,
        tm_id,
        category1_id,
        category1_name
    from dwd_dim_sku_info
    where date_format(dt,'yyyy-MM') = date_format('2020-06-25','yyyy-MM')
    group by id,tm_id,category1_id,category1_name
)
insert into ads_sale_tm_category1_stat_mn
select
    tm_id,
    category1_id,
    category1_name,
    sum(if(user_tm_count > 0,1,0)) buycount,
    sum(if(user_tm_count > 1,1,0)) buy_twice_last,
    sum(if(user_tm_count > 1,1,0)) / sum(if(user_tm_count > 0,1,0)) * 100 buy_twice_last_ratio,
    sum(if(user_tm_count > 2,1,0)) buy_3times_last,
    sum(if(user_tm_count > 2,1,0)) / sum(if(user_tm_count > 0,1,0)) * 100 buy_3times_last_ratio,
    date_format('2020-06-25','yyyy-MM'),
    '2020-06-25'
from
(
    select
        tm_id,
        category1_id,
        category1_name,
        user_id,
        sum(user_sku_count) user_tm_count
    from tmp_order
    join tmp_sku on tmp_order.sku_id = tmp_sku.id
    group by tm_id,category1_id,category1_name,user_id
) t1
group by tm_id,category1_id,category1_name;

--建表语句
create external table ads_area_topic(
    `dt` string COMMENT '统计日期',
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` bigint COMMENT '当天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_area_topic/';


--数据载入
insert into ads_area_topic
select
    '2020-06-25',
    id,
    province_name,
    area_code,
    iso_code,
    region_id,
    region_name,
    login_day_count,
    order_day_count,
    order_day_amount,
    payment_day_count,
    payment_day_amount
from dwt_area_topic;

show tables;


msck repair table ods_base_dic;

show tables;
--建表语句

--数据载入

