package org.ftang.zookeeping.speaker;

public interface NodeMonitorListener {
    public void startSpeaking();
    public void stopSpeaking();
    public String getProcessName();
}
