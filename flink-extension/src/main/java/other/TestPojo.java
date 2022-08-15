package other;

/**
 * @Author: Minc
 * @DateTime: 2022/8/15
 */
public class TestPojo {
    private String name;
    private Long time;
    private Integer num = 1;
    private Long startTime;
    private Long endTime;

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "TestPojo{" +
                "name='" + name + '\'' +
                ", time=" + time +
                ", num=" + num +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
