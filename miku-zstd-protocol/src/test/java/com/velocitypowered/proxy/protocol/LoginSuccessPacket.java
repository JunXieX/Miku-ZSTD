package com.velocitypowered.proxy.protocol;

/**
 * 测试用「登录期最后一个小包」：类名落在闸门的放行白名单前缀内
 * （{@code LoginSuccess}），用于验证闸门不会把它也扣住。
 */
public final class LoginSuccessPacket {

    public final int id;

    public LoginSuccessPacket(int id) {
        this.id = id;
    }
}
