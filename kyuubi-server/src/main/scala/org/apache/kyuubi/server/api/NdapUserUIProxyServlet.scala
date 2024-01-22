/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server.api

import java.net.URL
import java.util.Collections
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.{KYUUBI_VERSION, Logging}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient


private[api] class NdapUserUIProxyServlet(
    clonedConf: KyuubiConf
   ) extends ProxyServlet with Logging {

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    var targetURL = "/no-ui-error"
    extractTargetAddress(requestURI).foreach { case (user) =>
      val targetURI = requestURI.stripPrefix(s"/user-ui/$user") match {
        // for some reason, the proxy can not handle redirect well, as a workaround,
        // we simulate the Spark UI redirection behavior and forcibly rewrite the
        // empty URI to the Spark Jobs page.
        case "" | "/" => "/jobs/"
        case path => path
      }
      val targetQueryString =
        Option(request.getQueryString).filter(StringUtils.isNotEmpty).map(q => s"?$q").getOrElse("")
      val hostPort = cachedEngineURL.get(user).split(":")
      if (hostPort.nonEmpty) {
        targetURL = new URL(
          "http",
          hostPort(0),
          hostPort(1).toInt,
          targetURI + targetQueryString
        ).toString
      }
    }
    debug(s"rewrite $requestURL => $targetURL")
    targetURL
  }

  override def addXForwardedHeaders(
                                     clientRequest: HttpServletRequest,
                                     proxyRequest: Request): Unit = {
    val requestURI = clientRequest.getRequestURI
    extractTargetAddress(requestURI).foreach { case (user) =>
      // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
      proxyRequest.header("X-Forwarded-Context", s"/user-ui/$user")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private val r = "^/user-ui/([^/:]+)%40([^/:]+)/?.*".r
  private def extractTargetAddress(requestURI: String): Option[String] =
    requestURI match {
      case r(user, host) => Some(s"$user%40$host")
      case _ => None
    }

  private lazy val cachedEngineURL = CacheBuilder.newBuilder()
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .maximumSize(1000)
    .build(
      new CacheLoader[String, String] {
        def load(userName: String): String = {
          findUserEngineURL("spark", "user", "default", userName)
        }
      }
    )

  private def findUserEngineURL(
    engineType: String,
    shareLevel: String,
    subdomain: String,
    userName: String): String = {
    val engine = normalizeEngineInfo(userName, engineType, shareLevel, subdomain, "")
    val engineSpace = calculateEngineSpace(engine)

    val engineNodes = ListBuffer[ServiceNodeInfo]()
    withDiscoveryClient(clonedConf) { discoveryClient =>
      Option(subdomain).filter(_.nonEmpty) match {
        case Some(_) =>
          info(s"Listing engine nodes under $engineSpace")
          engineNodes ++= discoveryClient.getServiceNodesInfo(engineSpace)
        case None if discoveryClient.pathNonExists(engineSpace) =>
          warn(s"Path $engineSpace does not exist. user: $userName, engine type: $engineType, " +
            s"share level: $shareLevel, subdomain: $subdomain")
        case None =>
          discoveryClient.getChildren(engineSpace).map { child =>
            info(s"Listing engine nodes under $engineSpace/$child")
            engineNodes ++= discoveryClient.getServiceNodesInfo(s"$engineSpace/$child")
          }
      }
    }
    if (engineNodes.isEmpty) {
      return ""
    } else {
      return engineNodes.head.instance
    }
//    engineNodes.applyOrElse(0).instance
//    engineNodes.map(node =>
//        new Engine(
//          engine.getVersion,
//          engine.getUser,
//          engine.getEngineType,
//          engine.getSharelevel,
//          node.namespace.split("/").last,
//          node.instance,
//          node.namespace,
//          node.attributes.asJava))
//      .toSeq

  }

  private def normalizeEngineInfo(
                                   userName: String,
                                   engineType: String,
                                   shareLevel: String,
                                   subdomain: String,
                                   subdomainDefault: String): Engine = {
    // use default value from kyuubi conf when param is not provided
    Option(engineType).foreach(clonedConf.set(ENGINE_TYPE, _))
    Option(subdomain).filter(_.nonEmpty)
      .foreach(_ => clonedConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN, Option(subdomain)))
    Option(shareLevel).filter(_.nonEmpty).foreach(clonedConf.set(ENGINE_SHARE_LEVEL, _))

    val serverSpace = clonedConf.get(HA_NAMESPACE)
    val normalizedEngineType = clonedConf.get(ENGINE_TYPE)
    val engineSubdomain = clonedConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse(subdomainDefault)
    val engineShareLevel = clonedConf.get(ENGINE_SHARE_LEVEL)

    new Engine(
      KYUUBI_VERSION,
      userName,
      normalizedEngineType,
      engineShareLevel,
      engineSubdomain,
      null,
      serverSpace,
      Collections.emptyMap())
  }

  private def calculateEngineSpace(engine: Engine): String = {
    val userOrGroup = engine.getSharelevel match {
      case "GROUP" => engine.getUser
      case _ => engine.getUser
    }

    val engineSpace =
      s"${engine.getNamespace}_${engine.getVersion}_${engine.getSharelevel}_${engine.getEngineType}"
    DiscoveryPaths.makePath(engineSpace, userOrGroup, engine.getSubdomain)
  }
}
