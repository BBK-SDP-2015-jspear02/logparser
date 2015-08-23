package code;

public class IP {
    private String ipAddress, country, region, city;
    private long ipNumber;
    public IP(String ipAddress){
        this.ipAddress = ipAddress;
        getIpNumber();
    }
    public long getIpNumber() {
        String[] splitIpAddress = ipAddress.split("\\.");
        ipNumber = 0;
        for (int i = 0; i < splitIpAddress.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(splitIpAddress[i]);
            ipNumber += ip * Math.pow(256, power);
        }
        return ipNumber;
    }
    private void getGeoData() {
        //Run a query to get the geographical data
    }
    public String get(String item){
        return country;
    }
}
