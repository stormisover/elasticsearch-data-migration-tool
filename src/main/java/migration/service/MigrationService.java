package migration.service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import elasticsearch.ElasticRestClient;
import elasticsearch.constant.ESConstants;
import elasticsearch.api.resp.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

/**
 * Created by I319603 on 11/26/2016.
 */

@Service
public class MigrationService {
    private static final Logger logger = LogManager.getLogger(MigrationService.class);

    private RestClient client = null;//new ElasticRestClient().createInstance();
    private final Gson gson = new Gson();

    //bulk operation
    private final Integer STEP_SIZE = 400;
    // aggregate fields

    public void exportData(String fileName, Long tenantId, String EShosts) throws Exception {
        ElasticRestClient sclient = new ElasticRestClient();
        sclient.setHosts(Arrays.asList(EShosts));

        client = sclient.createInstance();
        export(tenantId, fileName);
    }

    public void importData(String EShosts, String file) throws Exception {
        ElasticRestClient sclient = new ElasticRestClient();
        sclient.setHosts(Arrays.asList(EShosts));
        client = sclient.createInstance();
        try {
            StringBuilder builder = new StringBuilder(20240);
            FileInputStream fstream = new FileInputStream(file);
            BufferedReader bstream = new BufferedReader(new InputStreamReader(fstream, "utf-8"));

            int line = 0;
            int totalCount = 0;
            String sCurrentLine;
            while ((sCurrentLine = bstream.readLine()) != null) {
                builder.append(sCurrentLine);
                builder.append("\r\n");
                line++;
                if (line == STEP_SIZE) {
                    ESBulkResponse response = doBulkRequest(builder.toString());
                    if (response != null && response.getErrors()) {
                        logger.warn("errors happen..need recheck it:" + response.getItems().toString() );
                    }
                    builder = new StringBuilder(20240);
                    line = 0;
                    totalCount += STEP_SIZE;
                }
            }

            if (line != 0) {
                logger.info("left ." + line);
                doBulkRequest(builder.toString());
                totalCount += line;
            }

            bstream.close();

            logger.info("total send " + totalCount/2);
        } catch (Exception e) {
            logger.error("error " + e);
        }

    }

    public JsonObject initSourceFilter(Long tenantId) {
        JsonObject source = new JsonObject();
        source.addProperty("size", STEP_SIZE);
        JsonObject query = new JsonObject();
        if (tenantId != -1) {
            JsonObject match = new JsonObject();
            match.addProperty("tenantId", tenantId);
            query.add("match", match);
        } else {
            query.add("match_all", new JsonObject());
        }

        source.add("query", query);
        return source;
    }

    public void export(Long tenantId, String file) {
        logger.info("Start to export data");
        Long totalCount = 0L;

        HashMap<String, String> param = new HashMap<>();
        param.put("scroll", "10m");
        if (tenantId != -1) {
            param.put("routing", tenantId.toString());
        }
        JsonObject source = initSourceFilter(tenantId);

        // get scroll info
        ESQueryResponse response = searchAll(param, source.toString());
        String scrollId = response.getScrollId();

        // scroll post body
        JsonObject scollSource = new JsonObject();
        scollSource.addProperty("scroll", "10m");
        scollSource.addProperty("scroll_id", scrollId);
        //BufferedWriter out = null;
        PrintWriter writer = null;
        try {
//            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file));
//            out = new BufferedWriter(writer,20480);
            writer = new PrintWriter(file, "utf-8");

            while(true) {
                if (response == null || response.getHit().getHits().size() == 0) {
                    logger.warn("Failed to get response, do nothing");
                    break;
                }

                // do update
                totalCount += doSaveData(writer, response);
                // check finish or not
                if (response.getHit().getHits().size() < STEP_SIZE) {
                    break;
                }

                // start new round update
                response = searchScroll(new HashMap<>(), scollSource.toString());
            }
        } catch (IOException e) {
            logger.error("Open file failed " + e);
        } finally {
            writer.flush();
            writer.close();
        }

        logger.info("data exported count = " + totalCount +" ");
        // need flush
        flush();
//        postCheckUpdate(totalCount);
        deleteScroll(scrollId);
    }

    public void postCheckUpdate(Long size) {
        if (size <= 0) {
            return;
        }

        // post check should be ok, during upgrade should be less 10000 new created product.
        if (size >= 10000) {
            size = 9999L;
        }

        logger.info("Post check update result");
        String body = "{  \"query\": {\"bool\" : {  \"must_not\" : {\"term\" : {  \"upgradeTag\" : 1612}  }}  }}";
        HashMap<String, String> param = new HashMap<>();
        param.put("size", size.toString());

        ESQueryResponse res = searchAll(param, body);
        if (res.getHit().getTotal() > 0) {
            logger.warn("Some product miss update, recover it totalCount=" + res.getHit().getTotal());

            // do it again
            doUpdate(res);
        }

        logger.info("Upgrade to 1612 end.");
    }

    public void flush() {
        try {
            client.performRequest(
                    "POST",
                    ESConstants.STORE_INDEX + "/_flush?wait_if_ongoing=true");
        } catch (IOException e) {
            logger.error("Error refresh request " + e);
        }
    }


    public Integer doSaveData(PrintWriter writer, ESQueryResponse response) {
        // use bulk API
        List<Hits> hits = response.getHit().getHits();
        JsonArray updateActionArray = new JsonArray();
        JsonArray updateDocArray = new JsonArray();

        for (int i = 0; i < hits.size(); ++i) {
            Hits hit = hits.get(i);
            Long tenantId = Long.parseLong(hit.getRouting());
            Long sourceId = hit.getSource().get("id").getAsLong();

            // action meta data
            JsonObject actionMeta = new JsonObject();
            JsonObject actionInnerObj = new JsonObject();
            actionInnerObj.addProperty("_id", sourceId);
            actionInnerObj.addProperty("routing", tenantId);
            actionInnerObj.addProperty("_type", hit.getType());
            actionInnerObj.addProperty("_index", hit.getIndex());
            if (hit.getType().equals("sku")) {
                actionInnerObj.addProperty("_parent", hit.getParent());
            }

            //actionInnerObj.addProperty("_retry_on_conflict",3);

            //actionMeta.add("update", actionInnerObj);
            actionMeta.add("index", actionInnerObj);
            updateActionArray.add(actionMeta);

            // doc source
//            JsonObject sourceObj = new JsonObject();
//            sourceObj.add("doc", hit.getSource());
//            sourceObj.addProperty("doc_as_upsert", true);
//            updateDocArray.add(sourceObj);
            updateDocArray.add(hit.getSource());
        }

        assert updateActionArray.size() == updateDocArray.size();

        StringBuilder body = new StringBuilder(1024*1024*20);
        for (int i = 0; i < updateActionArray.size(); ++i) {
            body.append(updateActionArray.get(i).toString());
            body.append("\r\n");
            body.append(updateDocArray.get(i).toString());
            body.append("\r\n");
        }

        try {
            writer.print(body.toString());
        } catch (Exception e) {
            logger.error("write failed need check again..." + e);
        }

        return updateActionArray.size();
    }

    /**
     * Use bulk api
     *   { "update" : {"_id" : "4"} }
         { "doc" : {"field" : "value"}}
     * @param response
     * @return
     */
    public Integer doUpdate(ESQueryResponse response) {
        List<Hits> hits = response.getHit().getHits();

        // use bulk API
        JsonArray updateActionArray = new JsonArray();
        JsonArray updateDocArray = new JsonArray();

        for (int i = 0; i < hits.size(); ++i) {
            Hits hit = hits.get(i);
            Long tenantId = Long.parseLong(hit.getRouting());
            Long productId = hit.getSource().get("id").getAsLong();

            JsonArray skuIds = hit.getSource().get("skuIds") == null ?
                    null : hit.getSource().get("skuIds").getAsJsonArray();
            JsonArray channelIds;
            if (skuIds != null) {
                channelIds = getSKUChannelIds(tenantId, skuIds);
            } else {
                channelIds = new JsonArray();
            }

            // action meta data
            JsonObject actionMeta = new JsonObject();
            JsonObject actionInnerObj = new JsonObject();
            actionInnerObj.addProperty("_id", productId);
            actionInnerObj.addProperty("routing", tenantId);
            actionMeta.add("update", actionInnerObj);
            updateActionArray.add(actionMeta);

            // doc source
            JsonObject sourceObj = new JsonObject();
            JsonObject sourceInnerObj = new JsonObject();
//            sourceInnerObj.add(CHANNELIDS, channelIds);
//            sourceInnerObj.addProperty(TENANTID, tenantId);
//            sourceInnerObj.addProperty(UPGRADE_TAG, UPGRADE_VERSION);
            sourceObj.add("doc", sourceInnerObj);
            updateDocArray.add(sourceObj);
        }

        assert updateActionArray.size() == updateDocArray.size();

        StringBuilder body = new StringBuilder(1024*1024*20);
        for (int i = 0; i < updateActionArray.size(); ++i) {
            body.append(updateActionArray.get(i).toString());
            body.append("\r\n");
            body.append(updateDocArray.get(i).toString());
            body.append("\r\n");
        }

        ESBulkResponse bulkResponse = doBulkRequest(body.toString());
        if (bulkResponse == null || bulkResponse.getErrors()) {
            logger.warn("Error happen in bulk request, need extra action");
        }

        return hits.size();
    }
    public ESBulkResponse doBulkRequestBytes(byte[] body) {
        try {
            HttpEntity requestBody = new StringEntity(new String(body), ContentType.APPLICATION_JSON);
            //BasicHeader header = new BasicHeader("Content-Type","application/json;charset=utf-8");
            Response response = client.performRequest(
                    "POST",
                    ESConstants.STORE_INDEX + "/_bulk",
                    new HashMap<String, String>(),
                    requestBody);

            ESBulkResponse esResponse = gson.fromJson(IOUtils.toString(response.getEntity().getContent(), "utf-8"),
                    ESBulkResponse.class);
            return esResponse;
        } catch (IOException e) {
            logger.error("Error bulk request " + e);
            logger.error("body is " + body);
        }

        return null;
    }

    public ESBulkResponse doBulkRequest(String body) {
        try {
            HttpEntity requestBody = new StringEntity(body, ContentType.APPLICATION_JSON);
            //BasicHeader header = new BasicHeader("Content-Type","application/json;charset=utf-8");
            Response response = client.performRequest(
                    "POST",
                    "/_bulk",
                    new HashMap<String, String>(),
                    requestBody);

            ESBulkResponse esResponse = gson.fromJson(IOUtils.toString(response.getEntity().getContent(), "utf-8"),
                    ESBulkResponse.class);
            return esResponse;
        } catch (IOException e) {
            logger.error("Error bulk request " + e);
            logger.error("body is " + body);
        }

        return null;
    }

    public JsonArray getSKUChannelIds(Long tenantId, JsonArray skuIds) {
        // get sku list info
        JsonObject queryIds = new JsonObject();
        queryIds.add("ids", skuIds);
        HashMap<String, String> param = new HashMap<String, String>();
        param.put("routing", tenantId.toString());

        ESMultiGetResponse response = multiGet(param, queryIds.toString());
        if (response == null) {
            logger.warn("Query sku info failed, no need do any update");
            return new JsonArray();
        }

        ArrayList<ESGetByIdResponse> responses = response.getDocs();
        JsonArray channelIds = new JsonArray();
        for (int i = 0; i < responses.size(); ++i) {
            ESGetByIdResponse res = responses.get(i);
            if (res.getFound()) {
                JsonObject sku = res.getObject();
                JsonArray channels = null;//sku.get(CHANNELIDS) == null ? null : sku.get(CHANNELIDS).getAsJsonArray();
                if (channels != null && channels.size() > 0) {
                    channels.forEach( channelId -> {
                        if (!channelIds.contains(channelId)) {
                            channelIds.add(channelId);
                        }
                    });
                }
            } else {
                logger.warn("Product with skuIds, but sku body missing");
            }
        }
        return channelIds;
    }

    public ESMultiGetResponse multiGet(HashMap<String, String> params, String requestBody) {
        try {
            HttpEntity requestEntity = new StringEntity(requestBody);
            Response response = client.performRequest(
                    "POST",
                    ESConstants.STORE_INDEX + "/" + ESConstants.SKU_TYPE + "/_mget",
                    params,
                    requestEntity);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.warn("Not found" + response.toString());
                return null;
            }
            String resStr = IOUtils.toString(response.getEntity().getContent(), "utf-8");
            return gson.fromJson(resStr, ESMultiGetResponse.class);
        } catch (IOException e) {
            logger.error("Failed to get document with type  " + e);
        }

        return null;
    }


    public ESQueryResponse searchAll(Map<String, String> params, String body) {
        try {
            HttpEntity entity = new StringEntity(body);
            Response response = client.performRequest(
                    "GET",
                    "/stores*/_search",
                    params,
                    entity);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode > 299) {
                logger.warn("Problem while search a document: {}" + response.getStatusLine().getReasonPhrase());
                return null;
            }

            ESQueryResponse esQueryResponse = gson.fromJson(IOUtils.toString(response.getEntity().getContent(), "utf-8"),
                    ESQueryResponse.class);
            return esQueryResponse;
        } catch (IOException e) {
            logger.error("update failed " + e);
        }

        return null;
    }

    public ESQueryResponse searchScroll(Map<String, String> params, String body) {
        try {
            HttpEntity entity = new StringEntity(body);
            Response response = client.performRequest(
                    "POST",
                    "/_search/scroll",
                    params,
                    entity);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode > 299) {
                logger.warn("Problem while indexing a document: {}" + response.getStatusLine().getReasonPhrase());
                return null;
            }

            ESQueryResponse esQueryResponse = gson.fromJson(IOUtils.toString(response.getEntity().getContent(), "utf-8"),
                    ESQueryResponse.class);
            return esQueryResponse;
        } catch (IOException e) {
            logger.error("update failed " + e);
        }

        return null;
    }

    public void deleteScroll(String scrollId) {
        JsonArray scrollIds = new JsonArray();
        scrollIds.add(new JsonPrimitive(scrollId));
        JsonObject source = new JsonObject();
        source.add("scroll_id", scrollIds);

        try {
            HttpEntity entity = new StringEntity(source.toString());
            client.performRequest(
                    "DELETE",
                    "/_search/scroll",
                    new HashMap<String, String>(),
                    entity);
        } catch (IOException e) {
            logger.error("deleteScroll failed " + e);
        }
    }

    public void updateIndexMaxResultWindow() {
        final String body = "{\"index\": {\"max_result_window\" : 500000}}";
        try {
            HttpEntity entity = new StringEntity(body.toString());
            client.performRequest(
                    "PUT",
                    ESConstants.STORE_INDEX + "/_settings",
                    new HashMap<String, String>(),
                    entity);
        } catch (IOException e) {
            logger.error("updateIndexMaxResultWindow failed " + e);
        }
    }
}
