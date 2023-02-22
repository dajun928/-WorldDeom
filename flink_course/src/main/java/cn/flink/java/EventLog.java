package cn.flink.java;

import lombok.*;

import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public   class EventLog{
    private long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String,String> eventInfo;
}


