package com.furui.domain;

import com.furui.constant.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(access = lombok.AccessLevel.PUBLIC) // 关键：指定构建器为 PUBLIC
public class Msg<T> implements Serializable {
    private Status status = Status.INSERT;

    private T data;
}
