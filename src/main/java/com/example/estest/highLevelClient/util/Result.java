package com.example.estest.highLevelClient.util;

import lombok.*;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 响应信息主体
 *
 */
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class Result<T> implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Getter
	@Setter
	private int status;

	@Getter
	@Setter
	private String msg;

	@Getter
	@Setter
	private T data;

	public static <T> Result<T> ok()
	{
		return restResult(null, CommonConstants.SUCCESS, CommonConstants.SUCCESS_MSG);
	}

	public static <T> Result<T> ok(T data)
	{
		return restResult(data, CommonConstants.SUCCESS, CommonConstants.SUCCESS_MSG);
	}

	public static <T> Result<T> ok(T data, String msg)
	{
		return restResult(data, CommonConstants.SUCCESS, msg);
	}
	public static <T> Result<T> ok(int status, String msg)
	{
		return restResult(null, status, msg);
	}

    public static <T> Result<T> ok(T data, int status, String msg)
    {
        return restResult(data, status, msg);
    }

	public static <T> Result<T> failed()
	{
		return restResult(null, CommonConstants.FAIL, null);
	}

	public static <T> Result<T> failed(String msg)
	{
		return restResult(null, CommonConstants.FAIL, msg);
	}

	public static <T> Result<T> failed(T data)
	{
		return restResult(data, CommonConstants.FAIL, null);
	}

	public static <T> Result<T> failed(T data, String msg)
	{
		return restResult(data, CommonConstants.FAIL, msg);
	}

	public static <T> Result<T> failed(T data, int status, String msg)
	{
		return restResult(data, status, msg);
	}

    public static <T> Result<T> failed(int status, String msg)
    {
        return restResult(null, status, msg);
    }

	private static <T> Result<T> restResult(T data, int status, String msg)
	{
		Result<T> apiResult = new Result<T>();
		apiResult.setStatus(status);
		apiResult.setData(data);
		apiResult.setMsg(msg);
		return apiResult;
	}
}
