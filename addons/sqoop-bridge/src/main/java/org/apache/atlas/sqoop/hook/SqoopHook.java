/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.sqoop.hook;


import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.hook.FailedMessagesLogger;
import org.apache.atlas.notification.MessageDeserializer;
import org.apache.atlas.notification.MessageVersion;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.VersionedMessage;
import org.apache.atlas.notification.VersionedMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification.JSONArraySerializer;
import org.apache.atlas.notification.AbstractNotification.ReferenceableSerializer;
import org.apache.atlas.notification.hook.HookMessageDeserializer;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.notification.hook.HookNotification.EntityCreateRequest;
import org.apache.atlas.sqoop.model.SqoopDataModelGenerator;
import org.apache.atlas.sqoop.model.SqoopDataTypes;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.sqoop.SqoopJobDataPublisher;
import org.apache.sqoop.util.ImportException;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * AtlasHook sends lineage information to the AtlasSever.
 */
public class SqoopHook extends SqoopJobDataPublisher {

	private static final Logger LOG = LoggerFactory.getLogger(SqoopHook.class);
	public static final String CONF_PREFIX = "atlas.hook.sqoop.";
	public static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

	public static final String ATLAS_CLUSTER_NAME = "atlas.cluster.name";
	public static final String DEFAULT_CLUSTER_NAME = "primary";

	/**
	 * Used for message serialization.
	 */
	public static final Gson GSON = new GsonBuilder().
			registerTypeAdapter(IReferenceableInstance.class, new ReferenceableSerializer()).
			registerTypeAdapter(Referenceable.class, new ReferenceableSerializer()).
			registerTypeAdapter(JSONArray.class, new JSONArraySerializer()).
			create();
	public static final MessageVersion CURRENT_MESSAGE_VERSION = new MessageVersion("1.0.0");


	static {
		org.apache.hadoop.conf.Configuration.addDefaultResource("sqoop-site.xml");
	}

	public Referenceable createHiveDatabaseInstance(String clusterName, String dbName)
			throws Exception {
		Referenceable dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
		dbRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
		dbRef.set(AtlasClient.NAME, dbName);
		dbRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
				HiveMetaStoreBridge.getDBQualifiedName(clusterName, dbName));
		return dbRef;
	}

	public Referenceable createHiveTableInstance(String clusterName, Referenceable dbRef,
			String tableName, String dbName) throws Exception {
		Referenceable tableRef = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
		tableRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
				HiveMetaStoreBridge.getTableQualifiedName(clusterName, dbName, tableName));
		tableRef.set(AtlasClient.NAME, tableName.toLowerCase());
		tableRef.set(HiveDataModelGenerator.DB, dbRef);
		return tableRef;
	}

	private Referenceable createDBStoreInstance(SqoopJobDataPublisher.Data data)
			throws ImportException {

		Referenceable storeRef = new Referenceable(SqoopDataTypes.SQOOP_DBDATASTORE.getName());
		String table = data.getStoreTable();
		String query = data.getStoreQuery();
		if (StringUtils.isBlank(table) && StringUtils.isBlank(query)) {
			throw new ImportException("Both table and query cannot be empty for DBStoreInstance");
		}

		String usage = table != null ? "TABLE" : "QUERY";
		String source = table != null ? table : query;
		String name = getSqoopDBStoreName(data);
		storeRef.set(AtlasClient.NAME, name);
		storeRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, name);
		storeRef.set(SqoopDataModelGenerator.DB_STORE_TYPE, data.getStoreType());
		storeRef.set(SqoopDataModelGenerator.DB_STORE_USAGE, usage);
		storeRef.set(SqoopDataModelGenerator.STORE_URI, data.getUrl());
		storeRef.set(SqoopDataModelGenerator.SOURCE, source);
		storeRef.set(SqoopDataModelGenerator.DESCRIPTION, "");
		storeRef.set(AtlasClient.OWNER, data.getUser());
		return storeRef;
	}

	private Referenceable createSqoopProcessInstance(Referenceable dbStoreRef, Referenceable hiveTableRef,
			SqoopJobDataPublisher.Data data, String clusterName) {
		Referenceable procRef = new Referenceable(SqoopDataTypes.SQOOP_PROCESS.getName());
		final String sqoopProcessName = getSqoopProcessName(data, clusterName);
		procRef.set(AtlasClient.NAME, sqoopProcessName);
		procRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sqoopProcessName);
		procRef.set(SqoopDataModelGenerator.OPERATION, data.getOperation());
		if (isImportOperation(data)) {
			procRef.set(SqoopDataModelGenerator.INPUTS, dbStoreRef);
			procRef.set(SqoopDataModelGenerator.OUTPUTS, hiveTableRef);
		} else {
			procRef.set(SqoopDataModelGenerator.INPUTS, hiveTableRef);
			procRef.set(SqoopDataModelGenerator.OUTPUTS, dbStoreRef);
		}
		procRef.set(SqoopDataModelGenerator.USER, data.getUser());
		procRef.set(SqoopDataModelGenerator.START_TIME, new Date(data.getStartTime()));
		procRef.set(SqoopDataModelGenerator.END_TIME, new Date(data.getEndTime()));

		Map<String, String> sqoopOptionsMap = new HashMap<>();
		Properties options = data.getOptions();
		for (Object k : options.keySet()) {
			sqoopOptionsMap.put((String)k, (String) options.get(k));
		}
		procRef.set(SqoopDataModelGenerator.CMD_LINE_OPTS, sqoopOptionsMap);

		return procRef;
	}

	static String getSqoopProcessName(Data data, String clusterName) {
		StringBuilder name = new StringBuilder(String.format("sqoop %s --connect %s", data.getOperation(),
				data.getUrl()));
		if (StringUtils.isNotEmpty(data.getStoreTable())) {
			name.append(" --table ").append(data.getStoreTable());
		}
		if (StringUtils.isNotEmpty(data.getStoreQuery())) {
			name.append(" --query ").append(data.getStoreQuery());
		}
		name.append(String.format(" --hive-%s --hive-database %s --hive-table %s --hive-cluster %s",
				data.getOperation(), data.getHiveDB().toLowerCase(), data.getHiveTable().toLowerCase(), clusterName));
		return name.toString();
	}

	static String getSqoopDBStoreName(SqoopJobDataPublisher.Data data)  {
		StringBuilder name = new StringBuilder(String.format("%s --url %s", data.getStoreType(), data.getUrl()));
		if (StringUtils.isNotEmpty(data.getStoreTable())) {
			name.append(" --table ").append(data.getStoreTable());
		}
		if (StringUtils.isNotEmpty(data.getStoreQuery())) {
			name.append(" --query ").append(data.getStoreQuery());
		}
		return name.toString();
	}

	static boolean isImportOperation(SqoopJobDataPublisher.Data data) {
		return data.getOperation().toLowerCase().equals("import");
	}

	@Override
	public void publish(SqoopJobDataPublisher.Data data) throws Exception {
		Configuration atlasProperties = ApplicationProperties.get();
		String clusterName = atlasProperties.getString(ATLAS_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);

		Referenceable dbStoreRef = createDBStoreInstance(data);
		Referenceable dbRef = createHiveDatabaseInstance(clusterName, data.getHiveDB());
		Referenceable hiveTableRef = createHiveTableInstance(clusterName, dbRef,
				data.getHiveTable(), data.getHiveDB());
		Referenceable procRef = createSqoopProcessInstance(dbStoreRef, hiveTableRef, data, clusterName);

		int maxRetries = atlasProperties.getInt(HOOK_NUM_RETRIES, 3);
		HookNotification.HookNotificationMessage message =
				new HookNotification.EntityCreateRequest(AtlasHook.getUser(), dbStoreRef, dbRef, hiveTableRef, procRef);
		LOG.info("SqoopJobDataPublisher:dbStoreRef:"+dbStoreRef.toString());
		LOG.info("SqoopJobDataPublisher:dbRef:"+dbRef.toString());
		LOG.info("SqoopJobDataPublisher:hiveTableRef:"+hiveTableRef.toString());
		LOG.info("SqoopJobDataPublisher:procRef:"+procRef.toString());

//		LOG.info("#####Starting to publish via TestClient######");
//		LOG.info("POST dbStoreREF");
//		testClient(dbStoreRef.toString());
//		LOG.info("POST dbRef");
//		testClient(dbRef.toString());
//		LOG.info("POST hiveTableRef");
//		testClient(hiveTableRef.toString());
//		LOG.info("POST procRef");
//		testClient(procRef.toString());
//		LOG.info("#####End publish via TestClient######");

		LOG.info("VW: Start to Notify Entities");
		AtlasHook.notifyEntities(Arrays.asList(message), maxRetries);
		LOG.info("VW: notifyEnties complete with message: "+message.toString());
		
		// get JSON and versioned messages array
		LOG.info("#####Starting to publish via TestClient######");
		String[] strMessages = new String[Arrays.asList(message).size()];
		for (int index = 0; index < Arrays.asList(message).size(); index++) {
			strMessages[index] = getMessageJson(Arrays.asList(message).get(index));
			LOG.info("VW: SQOOP HOOK :"+strMessages[index]);
		}
		LOG.info("VW: END OF SQOOP HOOK :"+Arrays.toString(strMessages));
		
		//TODO check for server
		
		//send to server
        for (String messageServe : strMessages) {
        	testClient(messageServe);
        	// on the server side. probably have similar behavior
        	LOG.info("VW: Serverside messageServe:" +messageServe);
            final MessageDeserializer<HookNotification.HookNotificationMessage> deserializer=new HookMessageDeserializer();
            HookNotification.HookNotificationMessage msg=deserializer.deserialize(messageServe);
            LOG.info("Deserilizer clasS: "+deserializer.getClass().getName());
            LOG.info("VW: Serverside msg deserialized:" +msg);
            LOG.info("VW: after deserializer:" +msg.getType().toString());
            HookNotification.EntityCreateRequest createRequest =
                    (HookNotification.EntityCreateRequest) msg;
            LOG.info("VW: createRequest: "+createRequest.toString());
            
            Configuration atlasConf = ApplicationProperties.get();
            String ATLAS_ENDPOINT = "atlas.rest.address";
            String[] atlasEndpoint = atlasConf.getStringArray(ATLAS_ENDPOINT);
            String DEFAULT_DGI_URL = "http://localhost:21000/";
            if (atlasEndpoint == null || atlasEndpoint.length == 0){
                atlasEndpoint = new String[] { DEFAULT_DGI_URL };
            }
            AtlasClient atlasClient;

            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
                atlasClient = new AtlasClient(atlasEndpoint, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                atlasClient = new AtlasClient(ugi, ugi.getShortUserName(), atlasEndpoint);
            }
            LOG.info("VW: CREATING Entity from Atlas Client:");
            LOG.info("VW: createRequestGetEntities: "+createRequest.getEntities());
            //JSONArray entityArray = new JSONArray(createRequest.getEntities().size());
            atlasClient.createEntity(createRequest.getEntities());
        }
        LOG.info("#####End publish via TestClient######");

	}

	public String testClient(String message) throws MalformedURLException, IOException
	{

		URL url = new URL("http://zulu.hdp.com:3333/");
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		Base64.Encoder encoder = Base64.getEncoder();
		//String encodedString = encoder.encodeToString((principal+":"+password).getBytes(StandardCharsets.UTF_8) );
		conn.setRequestMethod("POST");
		//conn.setRequestProperty  ("Authorization", "Basic " + encodedString);
		conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
		//conn.setUseCaches(false);
		conn.setDoInput(true);
		conn.setDoOutput(true);
		//
		OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
		osw.write(message);
		osw.flush();
		osw.close();
		//
		conn.connect();
		String resp = result(conn, true);
		conn.disconnect();
		LOG.info("testClient received response:"+resp);
		return resp;
	}

	private static String result(HttpURLConnection conn, boolean input) throws IOException {
		StringBuffer sb = new StringBuffer();
		if (input) {
			InputStream is = conn.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line = null;

			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			reader.close();
			is.close();
		}
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("code", conn.getResponseCode());
		result.put("mesg", conn.getResponseMessage());
		result.put("type", conn.getContentType());
		result.put("data", sb);
		//
		// Convert a Map into JSON string.
		//
		Gson gson = new Gson();
		String json = gson.toJson(result);
		LOG.info("json = " +prettyPrint(json));


		//
		// Convert JSON string back to Map.
		//
		// Type type = new TypeToken<Map<String, Object>>(){}.getType();
		// Map<String, Object> map = gson.fromJson(json, type);
		// for (String key : map.keySet()) {
		// System.out.println("map.get = " + map.get(key));
		// }

		return json;
	}
	protected static String prettyPrint(String jsonContent)  {
		String prettyJsonString=null;
		String data=null;
		if ((jsonContent == null) || jsonContent.isEmpty()) {
			return "EmptyJson";
		} else {
			try {
				JsonParser jp = new JsonParser();
				JsonElement je = jp.parse(jsonContent);

				if (je.isJsonObject())
				{
					JsonObject response = je.getAsJsonObject();
					data = response.get("data").getAsString();
				}
				prettyJsonString = data;
			} 
			catch (JsonSyntaxException e) {
				LOG.error(e.getMessage());
			}
			return prettyJsonString;
		}

	}
	public static String getMessageJson(Object message) {
		VersionedMessage<?> versionedMessage = new VersionedMessage<>(CURRENT_MESSAGE_VERSION, message);

		return GSON.toJson(versionedMessage);
	}
    @VisibleForTesting
    void handleMessage(HookNotification.HookNotificationMessage message) throws
        AtlasServiceException, AtlasException {
    	//build atlas Client
        AtlasClient atlasClient=null;
        for (int numRetries = 0; numRetries < 5; numRetries++) {
            LOG.debug("Running attempt {}", numRetries);
            try {
                //atlasClient.setUser(message.getUser());
                switch (message.getType()) {
                case ENTITY_CREATE:
                    HookNotification.EntityCreateRequest createRequest =
                        (HookNotification.EntityCreateRequest) message;
                    atlasClient.createEntity(createRequest.getEntities());
                    break;

                case ENTITY_PARTIAL_UPDATE:
                    HookNotification.EntityPartialUpdateRequest partialUpdateRequest =
                        (HookNotification.EntityPartialUpdateRequest) message;
                    atlasClient.updateEntity(partialUpdateRequest.getTypeName(),
                        partialUpdateRequest.getAttribute(),
                        partialUpdateRequest.getAttributeValue(), partialUpdateRequest.getEntity());
                    break;

                case ENTITY_DELETE:
                    HookNotification.EntityDeleteRequest deleteRequest =
                        (HookNotification.EntityDeleteRequest) message;
                    atlasClient.deleteEntity(deleteRequest.getTypeName(),
                        deleteRequest.getAttribute(),
                        deleteRequest.getAttributeValue());
                    break;

                case ENTITY_FULL_UPDATE:
                    HookNotification.EntityUpdateRequest updateRequest =
                        (HookNotification.EntityUpdateRequest) message;
                    atlasClient.updateEntities(updateRequest.getEntities());
                    break;

                default:
                    throw new IllegalStateException("Unhandled exception!");
                }

                break;
            } catch (Throwable e) {
                LOG.warn("Error handling message" + e.getMessage());
                try{
                    LOG.info("Sleeping for {} ms before retry", 10);
                    Thread.sleep(10);
                }catch (InterruptedException ie){
                    LOG.error("Notification consumer thread sleep interrupted");
                }

                if (numRetries == (5 - 1)) {
                    LOG.warn("Max retries exceeded for message {}", message, e);

                    return;
                }
            }
        }
        //commit();
    }

}
