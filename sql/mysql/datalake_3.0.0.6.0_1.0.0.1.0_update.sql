DROP PROCEDURE IF EXISTS `upgradeDB`;
delimiter //
CREATE PROCEDURE `upgradeDB`(in expectedVersion VARCHAR(50), in newVersion VARCHAR(50),out resultInfo VARCHAR(500))
BEGIN
    DECLARE currentVersion VARCHAR(50);
    DECLARE t_error INTEGER DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error=1;
    SELECT Version INTO currentVersion FROM `dbversion` ORDER BY Version DESC LIMIT 1;
    if currentVersion=expectedVersion then

        SET resultInfo='currentVersion=expectedVersion,can run upgrade sql ';

        START TRANSACTION;
-- 根据实际情况修改更新人和升级理由
        INSERT INTO `dbversion` VALUES (newVersion, 'lishuai', now(), 'stspro: save sql and threshold');

-- 实际升级语句开始 upgrade sql statement....

        create table if not exists sts_query_filter_threshold
        (
            id bigint auto_increment comment 'id'
                primary key,
            name varchar(64) null comment '名称',
            username varchar(64) null comment '用户名',
            threshold varchar(256) null comment '阈值',
            create_time datetime null comment '创建时间',
            update_time datetime null comment '更新时间',
            constraint username
                unique (username)
        )
            comment 'impala query filter 阈值';

        create table if not exists sts_saved_sql
        (
            id bigint auto_increment comment '主键ID'
                primary key,
            name varchar(64) null comment '名称',
            username varchar(64) not null comment '用户名',
            tenantname varchar(64) not null comment '租户名',
            statement mediumtext not null comment 'sql语句',
            create_time datetime null comment '创建时间',
            update_time datetime null comment '更新时间'
        );





-- 实际升级语句结束 upgrade sql statement....


        IF t_error = 1 THEN
            ROLLBACK;
        ELSE
            COMMIT;
        END IF;
        select t_error;
    ELSE
        SET resultInfo='currentVersion!=expectedVersion,can not run upgrade sql statement';
    end if;
END //

delimiter ;

-- -根据实际情况修改下面版本,一般跟文件名对应
CALL upgradeDB('3.0.0.6.0','1.0.0.1.0',@resultInfo);

SELECT @resultInfo;