package cn.flinksql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {
    private long guid;
    private String sessionId;
    private String eventId;
    private int eventTime;  // 行为时长
}


/***
 * {"guid":1,"sessionId":"s01","eventId":"e01","eventTime":1000}
 * {"guid":1,"sessionId":"s01","eventId":"e02","eventTime":2000}
 * {"guid":1,"sessionId":"s01","eventId":"e03","eventTime":3000}
 * {"guid":2,"sessionId":"s02","eventId":"e02","eventTime":2000}
 * {"guid":2,"sessionId":"s02","eventId":"e02","eventTime":3000}
 * {"guid":2,"sessionId":"s02","eventId":"e01","eventTime":4000}
 * {"guid":2,"sessionId":"s02","eventId":"e03","eventTime":5000}
 * ***/