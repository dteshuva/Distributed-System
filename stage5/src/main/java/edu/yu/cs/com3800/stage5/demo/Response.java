package edu.yu.cs.com3800.stage5.demo;

public class Response {

    private int num;
    private long serverId;
    private String content;
    private Exception error;

    public Response(long serverId, String content, int num) {
        this.serverId = serverId;
        this.content = content;
        this.num = num;
    }

    public Response(long serverId, Exception error) {
        this.serverId = serverId;
        this.error = error;
        this.num = num;
    }

    public long getServerId() {
        return serverId;
    }

    public String getContent() {
        return content;
    }

    public Exception getError() {
        return error;
    }

    @Override
    public String toString() {
        if (error != null) {
            return "Response from server ID " + serverId + " resulted in error: " + error.getMessage();
        } else {
            return "Response from server ID " + serverId + ": " + content;
        }
    }
}
