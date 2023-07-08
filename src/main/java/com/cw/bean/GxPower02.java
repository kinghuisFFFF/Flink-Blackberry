package com.cw.bean;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * @description gx_power_02
 * @author cw
 * @date 2023-07-06
 */
@Data
public class GxPower02 implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
    * id
    */
    private String id;

    /**
    * producer_id
    */
    private String producerId;
    /**
     *  工厂名称
     */
    private String producerName;

    /**
    * dev_group
    */
    private String devGroup;

    /**
    * htime
    */
    private String htime;

    /**
    * 每天各工序电量增量
    */
    private BigDecimal addActivePower;

    /**
    * crt
    */
    private Date crt;

    /**
     *  水印时间
     */
    private Long wm_htime;

    public GxPower02() {}
}