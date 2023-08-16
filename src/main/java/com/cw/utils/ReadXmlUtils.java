package com.cw.utils;

import cn.hutool.core.util.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPathConstants;

/**
 * @Title:
 * @BelongProjecet Flink-Blackberry
 * @BelongPackage com.cw.test
 * @Description:
 * @Copyright time company - Powered By 研发一部
 * @Author: cw
 * @Date: 2023/7/3 18:30
 * @Version V1.0
 */
public class ReadXmlUtils {
    public static void main(String[] args) {


//        m1();
        Document document = XmlUtil.readXML("sql-map/ods/SQL01.xml");
        String sql1 = getTagValueByPath(document, "//sqls/sql01");
        System.out.println(sql1);
        System.out.println(sql1);
    }

    /**
     * 根据xml标签路径获取标签值
     */
    public static String getTagValueByPath(Document document, String path) {
        return String.valueOf(XmlUtil.getByXPath(path, document, XPathConstants.STRING));
    }

    private static void m1() {
        String xmlData="<forms version=\"2.1\">\n" +
                "    <formExport>\n" +
                "        <summary id=\"1132755668421070367\" name=\"formmain_0031\"/>\n" +
                "        <definitions>\n" +
                "            <column id=\"field0001\" type=\"0\" name=\"field1\" length=\"255\"/>\n" +
                "            <column id=\"field0002\" type=\"0\" name=\"field2\" length=\"256\"/>\n" +
                "        </definitions>\n" +
                "        <values>\n" +
                "            <column name=\"field1\">\n" +
                "                <value>\n" +
                "                    建行一世\n" +
                "                </value>\n" +
                "            </column>\n" +
                "            <column name=\"field2\">\n" +
                "                <value>\n" +
                "                    CSDN\n" +
                "                </value>\n" +
                "            </column>\n" +
                "        </values>\n" +
                "        <subForms/>\n" +
                "    </formExport>\n" +
                "</forms>\n" +
                "\n";
        Document document= XmlUtil.parseXml(xmlData);
        //获得XML文档根节点
        Element elementG=XmlUtil.getRootElement(document);
        //打印节点名称
        System.out.println(elementG.getTagName());
        //获取下层节点（该方法默认取第一个）
        Element elementZ=XmlUtil.getElement(elementG,"formExport");
        System.out.println(elementZ.getTagName());
        //获取下层节点（该方法默认取第一个）
        Element elementZ1=XmlUtil.getElement(elementZ,"definitions");
        System.out.println(elementZ1.getTagName());
        //获取下层节点（该方法默认取第一个）
        Element elementZ2=XmlUtil.getElement(elementZ1,"column");
        System.out.println(elementZ2.getTagName());
        //读取属性length
        System.out.println(elementZ2.getAttribute("length"));
    }
}
