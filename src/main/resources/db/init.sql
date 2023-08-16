
DROP TABLE IF EXISTS `table_process`;
CREATE TABLE `table_process`  (
  `source_table` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '来源表',
  `operate_type` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '操作类型 insert,update,delete',
  `sink_type` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '输出类型 hbase kafka',
  `sink_table` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '输出表(主题)',
  `sink_columns` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '输出字段',
  `sink_pk` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '主键字段',
  `sink_extend` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '建表扩展',
  PRIMARY KEY (`source_table`, `operate_type`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
