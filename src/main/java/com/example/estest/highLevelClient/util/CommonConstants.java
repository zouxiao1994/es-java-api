package com.example.estest.highLevelClient.util;

/**
 */
public interface CommonConstants
{
	/**
	 * header 中租户ID
	 */
	String TENANT_ID = "TENANT_ID";
	/**
	 * 删除
	 */
	String STATUS_DEL = "1";
	/**
	 * 正常
	 */
	String STATUS_NORMAL = "0";

	/**
	 * 锁定
	 */
	String STATUS_LOCK = "9";

	/**
	 * 菜单
	 */
	String MENU = "0";

	/**
	 * 菜单树根节点
	 */
	Integer MENU_TREE_ROOT_ID = -1;

	/**
	 * 编码
	 */
	String UTF8 = "UTF-8";

	/**
	 * 成功标记
	 */
	Integer SUCCESS = 200;
	/**
	 * 成功时默认msg
	 */
	String SUCCESS_MSG = "OK";
	/**
	 * 失败标记
	 */
	Integer FAIL = -1;
	
	/**
     * 验证码前缀
     */
    String DEFAULT_CODE_KEY = "PATPAT_CODE_KEY_";
}
