package user_with_max_ratings;

public class MaxTo {

    public static String USER_ID = "userId";
    public static String MAX = "max";

    private String userId;
    private double max;

    public MaxTo() {
        userId = "";
        max = 0;
    }

    public String getUserId() {
        return userId;
    }

    private void setUserId(String userId) {
        this.userId = userId;
    }

    public double getMax() {
        return max;
    }

    private void setMax(double max) {
        this.max = max;
    }

    public void update(String userId, double max) {
        setUserId(userId);
        setMax(max);
    }
}
