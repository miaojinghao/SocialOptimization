
package com.sharethis.social.dlxdata;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Generated;
import com.google.gson.annotations.Expose;
import org.apache.commons.lang.builder.ToStringBuilder;

@Generated("org.jsonschema2pojo")
public class DLXData {

    @Expose
    private Float ver;
    @Expose
    private String stid;
    @Expose
    private String fpc;
    @Expose
    private String lang;
    @Expose
    private Gid gid;
    @Expose
    private Geo geo;
    @Expose
    private List<Integer> dlxs = new ArrayList<Integer>();

    /**
     * 
     * @return
     *     The ver
     */
    public Float getVer() {
        return ver;
    }

    /**
     * 
     * @param ver
     *     The ver
     */
    public void setVer(Float ver) {
        this.ver = ver;
    }

    /**
     * 
     * @return
     *     The stid
     */
    public String getStid() {
        return stid;
    }

    /**
     * 
     * @param stid
     *     The stid
     */
    public void setStid(String stid) {
        this.stid = stid;
    }

    /**
     * 
     * @return
     *     The fpc
     */
    public String getFpc() {
        return fpc;
    }

    /**
     * 
     * @param fpc
     *     The fpc
     */
    public void setFpc(String fpc) {
        this.fpc = fpc;
    }

    /**
     * 
     * @return
     *     The lang
     */
    public String getLang() {
        return lang;
    }

    /**
     * 
     * @param lang
     *     The lang
     */
    public void setLang(String lang) {
        this.lang = lang;
    }

    /**
     * 
     * @return
     *     The gid
     */
    public Gid getGid() {
        return gid;
    }

    /**
     * 
     * @param gid
     *     The gid
     */
    public void setGid(Gid gid) {
        this.gid = gid;
    }

    /**
     * 
     * @return
     *     The geo
     */
    public Geo getGeo() {
        return geo;
    }

    /**
     * 
     * @param geo
     *     The geo
     */
    public void setGeo(Geo geo) {
        this.geo = geo;
    }

    /**
     * 
     * @return
     *     The dlxs
     */
    public List<Integer> getDlxs() {
        return dlxs;
    }

    /**
     * 
     * @param dlxs
     *     The dlxs
     */
    public void setDlxs(List<Integer> dlxs) {
        this.dlxs = dlxs;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
