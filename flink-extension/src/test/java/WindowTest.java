import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

/**
 * @Author: Minc
 * @DateTime: 2022/8/12
 */
public class WindowTest {

    @Test
    public void testWin() throws Exception {
        Long a = 1660287663000L;
        Long interval=1000L*60*10;
        long l = a / interval * interval;
        System.out.println(l);
    }

    private Long getWindowEnd(long now, long winSize, long offset) throws Exception {
        return TimeWindow.getWindowStartWithOffset(now, offset, winSize);
    }

}
