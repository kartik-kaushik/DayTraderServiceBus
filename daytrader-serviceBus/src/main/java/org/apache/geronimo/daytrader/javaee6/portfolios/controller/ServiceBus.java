/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.geronimo.daytrader.javaee6.portfolios.controller;

// DayTrader
import org.apache.geronimo.daytrader.javaee6.portfolios.service.PortfoliosService;
import org.apache.geronimo.daytrader.javaee6.portfolios.utils.Log;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
// Java
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;

// Daytrader
import org.apache.geronimo.daytrader.javaee6.core.beans.RunStatsDataBean;
import org.apache.geronimo.daytrader.javaee6.entities.AccountDataBean;
import org.apache.geronimo.daytrader.javaee6.entities.HoldingDataBean;
import org.apache.geronimo.daytrader.javaee6.entities.OrderDataBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.http.HttpEntity;
// Spring
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.IQueueClient;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import lombok.extern.log4j.Log4j2;

/**
 * API endpoints are documented using Swagger UI. 
 * 
 * @see https://{portfoliosServiceHost}:{portfoliosServicePort}/swagger-ui.html
 * 
 * HTTP Methods. There is no official and enforced standard for designing HTTP & RESTful APIs.
 * There are many ways for designing them. These notes cover the guidelines used for designing
 * the aforementioned HTTP & RESTful API.
 * 
 * - GET is used for reading objects; no cache headers is used if the object should not be cached
 * 
 * - POST is used for creating objects or for operations that change the server side state. In the
 *   case where an object is created, the created object is returned; instead of it'd URI. This is 
 *   in keeping with the existing services. A better practice is to return the URI to the created
 *   object, but we elected to keep the REST APIs consistent with the existing services. New APIs 
 *   that return the URI can be added during Stage 04: Microservices if required. 
 *   
 * - PUT is used for updates (full)
 * 
 * - PATCH is used for updates (partial)
 * 
 * TODO:
 * 1.	Access Control
 *		The controller provides a centralized location for access control. Currently,
 *		the application does not check to see is a user is logged in before invoking
 *		a method. So access control checks should be added in Stage 04: Microservices   
 */

@Log4j2
@Component
public class ServiceBus implements Ordered
{
	private static PortfoliosService portfoliosService = new PortfoliosService();
	 
	private IQueueClient iqueue;
	    @Autowired
	    private RestTemplate restTemplate;
		OrderDataBean orderDataBean = null;
		
		private final Logger log = LoggerFactory.getLogger(ServiceBus.class); 
		   // private String connectionString ="Endpoint=sb://topicsinservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ny3fKCHs2eFllaAkmT4VUDw5F+r815o1P2ftwOhZLhI=";
			 
		ServiceBus(IQueueClient iq) {
				this.iqueue = iq;
			}
		    
		    
		    @EventListener(ApplicationReadyEvent.class)
		    public void consume() throws Exception {

		    	recievingmessages(iqueue);
		    	//recievingmessages(iSubscriptionClient2);
		    	//recievingmessages(iSubscriptionClient3);
		    	    	
		    }    
		    
		    
		    @SuppressWarnings("deprecation")
			public void recievingmessages(IQueueClient iqueueclient) throws InterruptedException, ServiceBusException {


		    	iqueueclient.registerMessageHandler(new IMessageHandler() {

		            @Override
		            public CompletableFuture<Void> onMessageAsync(IMessage message) {
		                
		            	log.info("received message " + new String(message.getBody()) + " with body ID " + message.getMessageId());
		            
		            	//JsonObject jsonObject = new JsonParser().parse(new String(message.getBody())).getAsJsonObject();
		            	
		            	
		            	try {
							Object obj = new JSONParser().parse(new String(message.getBody()));
							
							JSONObject jo = (JSONObject) obj;
							orderDataBean = new OrderDataBean();
							
							String userId = (String) jo.get("userId");
							orderDataBean.setSymbol((String) jo.get("symbol"));
							
							double value = Double.parseDouble((String) jo.get("quantity"));
							orderDataBean.setQuantity(value );
							
							orderDataBean.setPrice(new BigDecimal((String)jo.get("price")));
							orderDataBean.setOrderType((String)jo.get("buySell"));
							orderDataBean.setOrderStatus("open");
							
							log.info("Symbol is-->"+orderDataBean.getSymbol());
							log.info("Quantity is-->"+orderDataBean.getQuantity());
							log.info("Price is-->"+orderDataBean.getPrice());
							
							HttpHeaders headers = new HttpHeaders();
							headers.setContentType(MediaType.APPLICATION_JSON);
							
							HttpEntity<OrderDataBean> requestEntity = new HttpEntity<>(orderDataBean, headers);
							
							/*
							 * restTemplate.exchange("https://localhost:3443/portfolios/"+userId+"/orders",
							 * HttpMethod.POST, requestEntity, ResponseEntity.class);
							 */
							Integer mode = 12345;
							
							if (orderDataBean.getOrderType().equals("buy")) 
							{
								// Buy the specified quantity of stock and add a holding to the portfolio
								orderDataBean = portfoliosService.buy(userId,orderDataBean.getSymbol(),orderDataBean.getQuantity(),mode);
								Log.traceExit("ServiceBus.processOrder()");
								//return new ResponseEntity<OrderDataBean>(orderDataBean, getNoCacheHeaders(), HttpStatus.CREATED);
							}
							
							
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		            	
		                return CompletableFuture.completedFuture(null);
		            }
		            
		            @Override
		            public void notifyException(Throwable exception, ExceptionPhase phase) {
		                log.error("eeks!", exception);
		            }
		        });
		    	
		    	
		    }
		    	
		    	
		    @Override
		    public int getOrder() {
		        return Ordered.HIGHEST_PRECEDENCE;
		    }   	
		    	
		    	
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
	
	
	/**
	 * REST call to buy/sell a stock and add it to the user's portfolio.
	 * 
	 * TODO: 
	 * - Improved choreography and error handling. The group of actions to purchase a stock
	 *   must be considered as a single entity even though it crosses multiple microservices.
	 */
	@RequestMapping(value = "/portfolios/{userId}/orders", method = RequestMethod.POST)
	public ResponseEntity<OrderDataBean> processOrder(
			@PathVariable("userId") String userId, 
			@RequestBody OrderDataBean orderData,
			@RequestParam(value= "mode") Integer mode)
	{
		Log.traceEnter("ServiceBus.processOrder()");

		try // buy
		{			
			if (orderData.getOrderType().equals("buy")) 
			{
				// Buy the specified quantity of stock and add a holding to the portfolio
				orderData = portfoliosService.buy(userId,orderData.getSymbol(),orderData.getQuantity(),mode);
				Log.traceExit("ServiceBus.processOrder()");
				return new ResponseEntity<OrderDataBean>(orderData, getNoCacheHeaders(), HttpStatus.CREATED);
			}
		}
		catch (NotFoundException nfe)
		{
     		Log.error("ServiceBus.processOrder()", nfe);
			return new ResponseEntity<OrderDataBean>(HttpStatus.NOT_FOUND);			
		}
		catch (ClientErrorException cee)
		{
     		Log.error("ServiceBus.processOrder()", cee);
			return new ResponseEntity<OrderDataBean>(HttpStatus.CONFLICT);			
		}
		catch (Throwable t)
     	{
     		Log.error("ServiceBus.processOrder()", t);
			return new ResponseEntity<OrderDataBean>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		try // sell
		{
			if (orderData.getOrderType().equals("sell"))
			{
				// Sell the specified holding and remove it from the portfolio
				orderData = portfoliosService.sell(userId,orderData.getHoldingID(),mode);	
				Log.traceExit("ServiceBus.processOrder()");
				return new ResponseEntity<OrderDataBean>(orderData, getNoCacheHeaders(), HttpStatus.CREATED);
			}
		}
		catch (NotFoundException nfe)
		{
     		Log.error("ServiceBus.processOrder()", nfe);
			return new ResponseEntity<OrderDataBean>(HttpStatus.NOT_FOUND);			
		}
		catch (ClientErrorException cee)
		{
     		Log.error("ServiceBus.processOrder()", cee);
			return new ResponseEntity<OrderDataBean>(HttpStatus.CONFLICT);			
		}
		catch (Throwable t)
     	{
     		Log.error("ServiceBus.processOrder()", t);
			return new ResponseEntity<OrderDataBean>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		// other
		Throwable t = new BadRequestException("Invalid order type=" + orderData.getOrderType() + " valid types are 'buy' and 'sell'");
		Log.error("ServiceBus.processOrder()", t);
	    return new ResponseEntity<OrderDataBean>(HttpStatus.BAD_REQUEST);
	}
			
	
	//
	// Private helper functions
	//
	
	private HttpHeaders getNoCacheHeaders() 
	{
		HttpHeaders responseHeaders = new HttpHeaders();
		responseHeaders.set("Cache-Control", "no-cache");
		return responseHeaders;
	}
	
}