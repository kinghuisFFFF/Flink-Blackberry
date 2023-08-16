package com.cw.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.bean
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/7/13 9:46
 * @Version V1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaosConfig implements Serializable {
    private TaosBean taos;
}
