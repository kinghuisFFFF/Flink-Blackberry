/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.3.85_测试
 Source Server Type    : MySQL
 Source Server Version : 100406
 Source Host           : 192.168.3.85:21020
 Source Schema         : db_kafka2

 Target Server Type    : MySQL
 Target Server Version : 100406
 File Encoding         : 65001

 Date: 07/07/2023 09:25:03
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for table_process
-- ----------------------------
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

-- ----------------------------
-- Records of table_process
-- ----------------------------
INSERT INTO `table_process` VALUES ('gx_power_02', 'insert', 'kafka', 'dwd_gx_power_02', 'crt,htime,producer_id,add_active_power,dev_group,id', 'id', NULL);
INSERT INTO `table_process` VALUES ('gx_power_02', 'read', 'kafka', 'dwd_gx_power_02', 'crt,htime,producer_id,add_active_power,dev_group,id', 'id', NULL);
INSERT INTO `table_process` VALUES ('gx_power_02', 'update', 'kafka', 'dwd_gx_power_02', 'crt,htime,producer_id,add_active_power,dev_group,id', 'id', NULL);
INSERT INTO `table_process` VALUES ('host', 'insert', 'hbase', 'dim_host', 'serial_no,pwd,doffing_car,schedule_group_id,drawing_fan,fac_device_num,complicated_blade,area_id,rotor_fan,order_id,can_height,tru_name,process_fan,maitainer_id,guide_mouth,bind_group,single_spindle_plate,owner_id,block_size,row_index,comp_clean,address,navel,department,traverse,model_id,carding_kun,dev_type,transducer,type,rotor_bearing,\"index\",crt,others,unit_name,wind_fan,id,status,suction_car,doff_group,carding_fan,online,winding_form,brand,complicated_fan,nameplate,damping_cylinder,unit_group,middle_plate,mf_id,complicated_channel,dev_group,plc,running_status,res_twister_size,column_index,electric_clean,location,comb_channel,area,strain,alias,rotor,name,lut,sale_id,protocol,spindle_num', 'id', NULL);
INSERT INTO `table_process` VALUES ('host', 'read', 'hbase', 'dim_host', 'serial_no,pwd,doffing_car,schedule_group_id,drawing_fan,fac_device_num,complicated_blade,area_id,rotor_fan,order_id,can_height,tru_name,process_fan,maitainer_id,guide_mouth,bind_group,single_spindle_plate,owner_id,block_size,row_index,comp_clean,address,navel,department,traverse,model_id,carding_kun,dev_type,transducer,type,rotor_bearing,\"index\",crt,others,unit_name,wind_fan,id,status,suction_car,doff_group,carding_fan,online,winding_form,brand,complicated_fan,nameplate,damping_cylinder,unit_group,middle_plate,mf_id,complicated_channel,dev_group,plc,running_status,res_twister_size,column_index,electric_clean,location,comb_channel,area,strain,alias,rotor,name,lut,sale_id,protocol,spindle_num', 'id', NULL);
INSERT INTO `table_process` VALUES ('host', 'update', 'hbase', 'dim_host', 'serial_no,pwd,doffing_car,schedule_group_id,drawing_fan,fac_device_num,complicated_blade,area_id,rotor_fan,order_id,can_height,tru_name,process_fan,maitainer_id,guide_mouth,bind_group,single_spindle_plate,owner_id,block_size,row_index,comp_clean,address,navel,department,traverse,model_id,carding_kun,dev_type,transducer,type,rotor_bearing,\"index\",crt,others,unit_name,wind_fan,id,status,suction_car,doff_group,carding_fan,online,winding_form,brand,complicated_fan,nameplate,damping_cylinder,unit_group,middle_plate,mf_id,complicated_channel,dev_group,plc,running_status,res_twister_size,column_index,electric_clean,location,comb_channel,area,strain,alias,rotor,name,lut,sale_id,protocol,spindle_num', 'id', NULL);
INSERT INTO `table_process` VALUES ('producer', 'insert', 'hbase', 'dim_producer', 'email,contact_mobile,crt,warning,sale_id,hbase,org_code,area_id,postcode,name,lat,id,district,province,telephone,lut,contact_name,status,msg,parent_id,shift,website,flag,lng,manufacturer_id,address,city', 'id', NULL);
INSERT INTO `table_process` VALUES ('producer', 'read', 'hbase', 'dim_producer', 'email,contact_mobile,crt,warning,sale_id,hbase,org_code,area_id,postcode,name,lat,id,district,province,telephone,lut,contact_name,status,msg,parent_id,shift,website,flag,lng,manufacturer_id,address,city', 'id', NULL);
INSERT INTO `table_process` VALUES ('producer', 'update', 'hbase', 'dim_producer', 'email,contact_mobile,crt,warning,sale_id,hbase,org_code,area_id,postcode,name,lat,id,district,province,telephone,lut,contact_name,status,msg,parent_id,shift,website,flag,lng,manufacturer_id,address,city', 'id', NULL);

SET FOREIGN_KEY_CHECKS = 1;
