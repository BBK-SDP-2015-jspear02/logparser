package code;

public class IP {
    private String ipAddress, country, region, city;
    private long ipNumber;
    public IP(String ipAddress){
        this.ipAddress = ipAddress;
        getIpNumber();
    }
    public long getIpNumber() {
        String[] ipAddressInArray = this.ipAddress.split("\\.");
        long result = 0;
        long ip = 0;
        for (int x = 3; x >= 0; x--) {
            ip = Long.parseLong(ipAddressInArray[3 - x]);
            result |= ip << (x << 3);
        }

        return result;
    }


    private void getGeoData() {
        //Run a query to get the geographical data
    }
    public String get(String item){
        return country;
    }
}
