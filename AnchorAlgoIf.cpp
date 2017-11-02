#include <time.h> 
#include <algorithm>
#include "AnchorAlgoIf.h"
#include "UserWatchedInterface.h"
#include "UserRecommendedInterface.h"

#define DURING_ONE_DAY 60 * 60 * 24
#define DURING_ONE_HOUR  60 * 60
#define SCORE_TOP_COUNTER_DEFAULT 1
#define GETLIST_RETRY_THRESHOLD 3
#define GETLIST_RETRY_INTERVAL  10
#define REDIS_STATUS_READY_HOUR 7

AnchorAlgoIf::AnchorAlgoIf(int argc, char **argv)
: m_rcProxyS2SName("yycf_rs_user_rcproxy_d")
, m_filterS2SName("yycf_rs_vlfilter_d")
, m_videoListS2SName("yycf_rs_videolist_reader_d")
, m_configName("yycf_rs_algo_anchorcf_d")
, m_scoreTopCounter(SCORE_TOP_COUNTER_DEFAULT)
, m_simUsingCounter(10)
, m_simScore(0.7)
, m_simTablePrefix("sim-")
, m_scoreTablePrefix("ufo-")
, m_status(0)
, m_statfiltered()
, m_scoreRedisNotReadyCnt(0)
, m_simRedisNotReadyCnt(0)
, m_firstSyncOwnerInfo(false)
, m_videoInfo()
, m_localRecommmededVid()
, m_localRecommmededVidTs()
, m_scoreRecommendPool()
, m_scoreRecommendPoolTs()
, m_uid2AnchorList()
, m_updateVidInfoTimer(this, 1000 * 60)
, m_updateOwnerInfoTimer(this, 1000 * 60 * 60)
, m_checkRecommendTimer(this, 1000 * 1)
, m_checkRecommededVidTimer(this, 1000 * 60)
, m_checkRedisStatusTimer(this, 1000 * 60)
, m_onMetricsTimer(this, 1000 * 30)
{
  //algo redis init
  std::string scoreRedisMasterIp = "221.228.105.75";
  uint16_t scoreRedisMasterPort = 4017;
  std::string scoreRedisSlaveIp = "183.36.123.213";
  uint16_t scoreRedisSlavePort = 4012;

  std::string simRedisMasterIp = "221.228.105.75";
  uint16_t simRedisMasterPort = 4011;
  std::string simRedisSlaveIp = "183.3.211.111";
  uint16_t simRedisSlavePort = 6389;

  MainArgWrapper::getInstance()->GetOptVal("--redisScoreMasterIP", scoreRedisMasterIp);
  MainArgWrapper::getInstance()->GetOptVal("--redisScoreMasterPort", scoreRedisMasterPort);
  MainArgWrapper::getInstance()->GetOptVal("--redisScoreSlaveIP", scoreRedisSlaveIp);
  MainArgWrapper::getInstance()->GetOptVal("--redisScoreSlavePort", scoreRedisSlavePort);

  MainArgWrapper::getInstance()->GetOptVal("--redisSimMasterIP", simRedisMasterIp);
  MainArgWrapper::getInstance()->GetOptVal("--redisSimMasterPort", simRedisMasterPort);
  MainArgWrapper::getInstance()->GetOptVal("--redisSimSlaveIP", simRedisSlaveIp);
  MainArgWrapper::getInstance()->GetOptVal("--redisSimSlavePort", simRedisSlavePort);

  m_pAlgoScoreRedisClient = YFMakeShared<YFRedisRpcClient>("", 0);
  m_pAlgoScoreRedisClient->add(aton_addr(scoreRedisMasterIp), scoreRedisMasterPort);
  m_pAlgoScoreRedisClient->add(aton_addr(scoreRedisSlaveIp), scoreRedisSlavePort);

  m_pAlgoSimRedisClient = YFMakeShared<YFRedisRpcClient>("", 0);
  m_pAlgoSimRedisClient->add(aton_addr(simRedisMasterIp), simRedisMasterPort);
  m_pAlgoSimRedisClient->add(aton_addr(simRedisSlaveIp), simRedisSlavePort);

  FUNLOG(Info, "redis init ready");

  if (!isSimRedisReady(time(NULL) - 1 * 24 * 3600))
  {
    if (!isSimRedisReady(time(NULL) - 2 * 24 * 3600))
    {
      FUNLOG(Error, "sim redis is not ready");
      exit(0);
    }
  }

  //yy handler init
  MainArgWrapper::getInstance()->GetOptVal("--m_rcProxyS2SName", m_rcProxyS2SName);
  MainArgWrapper::getInstance()->GetOptVal("--m_filterS2SName", m_filterS2SName);
  MainArgWrapper::getInstance()->GetOptVal("--m_videoListS2SName", m_videoListS2SName);
  
  m_userBehaviorClient.init(m_rcProxyS2SName);
  m_vedioListFilterClient.init(m_filterS2SName);
  m_videoListClient.init(m_videoListS2SName);

  //init config center
  std::set<std::string> names;
  MainArgWrapper::getInstance()->GetOptVal("--configName", m_configName);
  names.insert(m_configName);
  MainConfigWrapper::threadInstance()->subscribe(this, names);

  //init video info 
  syncVidInfo();
}

uint16_t AnchorAlgoIf::onGetRecommendListByGuid(PSS_GetAlgoRecommendVideoListV2 * pReq, PSS_GetAlgoRecommendVideoListV2Res * pRes, const ConnInfo & connInfo)
{
  FUNLOG(Info, "uid:%s, req#:%u, excludeVid#:%zu, from=%s:%s",
         pReq->guid.sGuid.c_str(), pReq->count, pReq->excludeVids.size(), connInfo.getIpPort(), pReq->dump().c_str());

  preFilterByRpc(pReq->guid);

  pRes->context = pReq->context;
  pRes->guid = pReq->guid;

  std::vector<uint64_t> vids;

  std::string output;
  if (selectVid(pReq->guid, pReq->count, pReq->excludeVids, vids, output))
  {
    FUNLOG(Info, "selected, uid:%s vids#:%zu(%s)", pReq->guid.sGuid.c_str(), vids.size(), output.c_str());
  }
  else
  {
    uint32_t counter = buildSimPool(pReq->guid, pReq->excludeVids);

    FUNLOG(Info, "create pool, uid:%s pool#:%zu vids#:%u",
           pReq->guid.sGuid.c_str(), m_scoreRecommendPool.size(), counter);

    selectVid(pReq->guid, pReq->count, pReq->excludeVids, vids, output);
    FUNLOG(Info, "selected, uid:%s reqCounter:%u vids#:%zu(%s)",
           pReq->guid.sGuid.c_str(), pReq->count, vids.size(), output.c_str());
  }

  ostringstream os;
  for (auto& v : vids)
  {
    pRes->vidsrcs.push_back(VidSrc(v, DSRC_ALGO_ANCHORCF));
    os << v << ",";
  }

  FUNLOG(Info, "select all, uid:%s reqCounter:%u vidsrcs#:%zu(%s)",
         pReq->guid.sGuid.c_str(), pReq->count, pRes->vidsrcs.size(), os.str().c_str());

  return PROC_RES_OK;
}

uint16_t AnchorAlgoIf::onGetRecommendListByVid(PSS_GetItemCFRecommendVidsByVid * pReq, PSS_GetItemCFRecommendVidsByVidRes * pRes, const ConnInfo & connInfo)
{
  //预更新视频池
  preFilterByRpc(pReq->guid);

  FUNLOG(Info, "uid:%s, req#:%zu, excludeVid#:%zu, from=%s:%s",
         pReq->guid.sGuid.c_str(), pReq->vids.size(), pReq->excludeVids.size(),
         connInfo.getIpPort(), pReq->dump().c_str());

  pRes->context = pReq->context;
  pRes->guid = pReq->guid;

  std::map<uint64_t, std::vector<uint64_t> > simAnchors;
  std::map<uint64_t, std::map<uint64_t, std::vector<uint64_t> > > candidatePool;//vid-ownerid-vids
  std::vector<uint64_t> toFilterVids;
  std::vector<uint64_t> filteredVids;
  std::map<uint64_t, std::vector<VidSrc> > candidateList;//vid-vidsrcs

  //获取相似主播视频池
  for (auto& v : pReq->vids)
  {
    std::vector<uint64_t> anchors;
    uint64_t anchor = getAnchorId(v.first);
    if (anchor == 0)
      continue;

    if (!getSimAnchors(anchor, anchors))
      continue;

    ostringstream os;
    std::copy(anchors.begin(), anchors.end(), std::ostream_iterator<uint64_t>(os, ","));
    FUNLOG(Info, "uid:%s, anchor:%ju, owners#%zu(%s)", pReq->guid.sGuid.c_str(), anchor, anchors.size(), os.str().c_str());

    simAnchors[v.first] = anchors;

    for (auto& sa : anchors)
    {
      std::vector<uint64_t>& refvids = m_anchorInfo[sa];
      candidatePool[v.first][sa] = refvids;
      std::copy(refvids.begin(), refvids.end(), std::back_inserter(toFilterVids));
    }
  }

  //过滤
  filterByRpc(pReq->guid, toFilterVids, filteredVids);
  localFilter(pReq->guid, toFilterVids, filteredVids);
  ostringstream os;
  std::copy(filteredVids.begin(), filteredVids.end(), std::ostream_iterator<uint64_t>(os, ","));
  FUNLOG(Info, "uid:%s, filtered#%zu(%s)", pReq->guid.sGuid.c_str(), filteredVids.size(), os.str().c_str());

  for (auto it = candidatePool.begin(); it != candidatePool.end(); ++it)
  {
    std::map<uint64_t, std::vector<uint64_t> >& refMap = it->second;
    for (auto itref = refMap.begin(); itref != refMap.end();)
    {
      std::vector<uint64_t>& refVids = itref->second;
      for (auto& f : filteredVids)
        refVids.erase(std::remove(refVids.begin(), refVids.end(), f), refVids.end());

      if (refVids.empty())
        itref = refMap.erase(itref);
      else
        ++itref;
    }
  }

  //每个相似主播只保留一个vid
  for (auto& v : pReq->vids)
  {
    for (auto& sa : simAnchors[v.first])
    {
      std::vector<uint64_t>& refVids = candidatePool[v.first][sa];

      if (refVids.empty())
        continue;

      candidateList[v.first].push_back(VidSrc(*refVids.begin(), DSRC_ALGO_ANCHORCF));
      FUNLOG(Info, "uid:%s, reqVid:%ju, ownerId:%ju, vid:%ju",
             pReq->guid.sGuid.c_str(), v.first, sa, *refVids.begin());
    }
  }

  //返回请求响应
  std::vector<uint64_t> resVids;
  for (auto& v : pReq->vids)
  {
    std::vector<VidSrc>& refVids = candidateList[v.first];
    uint32_t pos = std::min((uint32_t)v.second, (uint32_t)refVids.size());
    std::copy(refVids.begin(), refVids.begin() + pos, std::back_inserter(pRes->vidsrcs[v.first]));

    os.str("");
    for (auto& vs : pRes->vidsrcs[v.first])
    {
      resVids.push_back(vs.vid);
      os << vs.vid << ",";
    }

    FUNLOG(Info, "uid:%s vid:%ju res#%zu(%s)",
           pReq->guid.sGuid.c_str(), v.first, pRes->vidsrcs[v.first].size(), os.str().c_str());
  }

  addLocalFilter(pReq->guid, resVids);

  return PROC_RES_OK;
}

uint32_t AnchorAlgoIf::buildSimPool(const UserId& uid, const std::set<uint64_t>& excludeVids)
{
  uint32_t retCounter = 0;

  //获取用户评分排序后anchors
  std::vector<uint64_t> anchors;
  if (!getAnchorsScore(uid, anchors))
    return retCounter;

  std::vector<uint64_t> simAnchors;
  std::map<uint64_t, std::vector<uint64_t> > candidatePool;
  std::vector<uint64_t> toFilterVids;
  std::vector<uint64_t> filteredVids;

  for (auto& v : anchors)
  {
    //计算主播的相似度
    //mysql插入保证相似度降序排序
    if (!getSimAnchors(v, simAnchors))
      continue;

    for (auto& sa : simAnchors)
    {
      std::vector<uint64_t>& refvids = m_anchorInfo[sa];
      for (auto& rv : refvids)
      {
        candidatePool[sa].push_back(rv);
        toFilterVids.push_back(rv);
      }
    }
  }

  //更新视频池
  filterByRpc(uid, toFilterVids, filteredVids);

  for (auto it = candidatePool.begin(); it != candidatePool.end();)
  {
    std::vector<uint64_t>& refVids = it->second;
    for (auto& f : filteredVids)
    {
      refVids.erase(std::remove(refVids.begin(), refVids.end(), f), refVids.end());
    }

    if (refVids.empty())
    {
      it = candidatePool.erase(it);
    }
    else
    {
      retCounter += refVids.size();
      ++it;
    }
  }

  if (m_scoreRecommendPool[uid.sGuid].empty())
    m_scoreRecommendPoolTs[getSecTime()].insert(uid.sGuid);
  for (auto& cp : candidatePool)
    m_scoreRecommendPool[uid.sGuid][cp.first] = cp.second;
    

  //更新主播链表
  for (auto rit = anchors.rbegin(); rit != anchors.rend(); ++rit)
    m_uid2AnchorList[uid.sGuid].push_front(*rit);

  //输出日志
  std::ostringstream os;
  std::copy(anchors.begin(), anchors.end(), std::ostream_iterator<uint64_t>(os, ","));
  FUNLOG(Info, "uid:%s, %zu(%s)", uid.sGuid.c_str(), anchors.size(), os.str().c_str());

  return retCounter;
}

bool AnchorAlgoIf::selectVid(const UserId& uid, uint32_t reqCounter,
                             const std::set<uint64_t>& excludeVids, std::vector<uint64_t>& vids, std::string& output)
{
  auto it = m_scoreRecommendPool.find(uid.sGuid);
  if (it == m_scoreRecommendPool.end())
    return false;

  std::map<uint64_t, std::vector<uint64_t> >& anchor2Vids = it->second;
  ostringstream oss;
  std::list<uint64_t>& anchorlist = m_uid2AnchorList[uid.sGuid];
  for (auto& sl : anchorlist)
    oss << sl << ",";
  FUNLOG(Info, "uid:%s reqCounter %u, svidStatuslist#%zu(%s)",
         uid.sGuid.c_str(), reqCounter, anchorlist.size(), oss.str().c_str());

  uint32_t counter = 0;

  std::list<uint64_t> tempStatusList = anchorlist;
  auto movePos = tempStatusList.begin();

  while (counter < reqCounter && movePos != tempStatusList.end())
  {
    uint64_t anchor = anchorlist.front();

    if (anchor2Vids.count(anchor) != 0)
    {
      std::vector<uint64_t> eraseVids;
      std::vector<uint64_t> candidateVids;
      std::vector<uint64_t>& refvlist = anchor2Vids[anchor];
      uint32_t pos = std::min((uint32_t)refvlist.size(), m_scoreTopCounter * 2);
      std::copy(refvlist.begin(), refvlist.begin() + pos, std::back_inserter(candidateVids));

      for (auto it = candidateVids.begin(); it != candidateVids.end();)
      {
        if (isExcludeFilter(*it, excludeVids))
          it = candidateVids.erase(it);
        else
          ++it;
      }

      uint32_t post = std::min((uint32_t)candidateVids.size(), m_scoreTopCounter);
      uint32_t leftpos = std::min(reqCounter - counter, post);
      std::copy(candidateVids.begin(), candidateVids.begin() + leftpos, std::back_inserter(vids));
      std::copy(candidateVids.begin(), candidateVids.begin() + leftpos, std::back_inserter(eraseVids));
      for (auto& v : eraseVids)
        refvlist.erase(std::remove(refvlist.begin(), refvlist.end(), v), refvlist.end());
      counter += leftpos;
    }

    anchorlist.pop_front();
    anchorlist.push_back(anchor);

    ++movePos;
  }

  std::ostringstream os;
  std::copy(vids.begin(), vids.end(), std::ostream_iterator<uint64_t>(os, ","));
  output = os.str();

  return true;
}

bool AnchorAlgoIf::getAnchorsScore(const UserId& uid, std::vector<uint64_t>& anchors)
{
  PRedisAnchorScoreListReq req(m_scoreTablePrefix, uid.sGuid);
  YFFuture<PRedisAnchorScoreListRes> f = m_pAlgoScoreRedisClient->call<YF_RedisOwnerScoreList>(&req);
  f.setTimeoutMs(200);
  if (f.get())
  {
    const PRedisAnchorScoreListRes& res = *(f.getValuePtr());
    if (res.resCode == 0)
    {
      std::list<std::pair<uint64_t, float> > anchor2Scores;
      for (auto& v : res.anchor2score)
      {
        anchor2Scores.push_back(make_pair(v.first, v.second));
      }

      //按score降序排列
      anchor2Scores.sort([](const std::pair<uint64_t, float>& x, const std::pair<uint64_t, float>& y)
      {
        return x.second > y.second;
      });

      //去掉重复&输出
      ostringstream os;
      for (auto& v : anchor2Scores)
      {
        os << v.first << ",";
        if (std::find(anchors.begin(), anchors.end(), v.first) != anchors.end())
          continue;
        anchors.push_back(v.first);
      }

      FUNLOG(Info, "uid:%s anchors#:%zu(%s)", uid.sGuid.c_str(), anchors.size(), os.str().c_str());
      return true;
    }

    YFMetrics()->incCounter("AnchorAlgoIfError", "getVidSimilarity", 1, "all");
    FUNLOG(Error, "uid:%s, res:%i", uid.sGuid.c_str(), res.resCode);
    return false;
  }

  YFMetrics()->incCounter("AnchorAlgoIfError", "getVidSimilarity", 1, "all");
  FUNLOG(Error, "uid:%s", uid.sGuid.c_str());
  return false;
}

bool AnchorAlgoIf::getSimAnchors(uint64_t anchor, std::vector<uint64_t>& simAnchors)
{
  PRedisAnchorSimilarityReq req(m_simTablePrefix, anchor);
  YFFuture<PRedisAnchorSimilarityRes> f = m_pAlgoSimRedisClient->call<YF_PRedisAnchorSimilarity>(&req);
  f.setTimeoutMs(100);

  if (f.get())
  {
    const PRedisAnchorSimilarityRes& res = *(f.getValuePtr());
    if (res.resCode == 0)
    {
      uint32_t counter = 0;
      for (auto& v : res.anchor2similarity)
      {
        if (++counter > m_simUsingCounter)
          break;

        if (v.second < m_simScore)
          continue;

        if (m_anchorInfo.count(v.first) == 0)
          continue;

        simAnchors.push_back(v.first);
      }
      return true;
    }

    YFMetrics()->incCounter("AnchorAlgoIfError", "getVidSimilarity", 1, "all");
    FUNLOG(Error, "anchor:%ju, res:%i", anchor, res.resCode);
    return false;
  }

  YFMetrics()->incCounter("AnchorAlgoIfError", "getVidSimilarity", 1, "all");
  FUNLOG(Error, "anchor:%ju", anchor);
  return false;
}

uint64_t AnchorAlgoIf::getAnchorId(uint64_t vid)
{
  auto it = m_videoInfo.find(vid);
  if (it != m_videoInfo.end())
    return it->second.ownerUid;
  return 0;
}

void AnchorAlgoIf::preFilterByRpc(const UserId& uid)
{
  PSS_VideoListFilterV2 req;
  req.guid = uid;
  req.vids.insert(0);
  auto f = m_vedioListFilterClient.call<PSS_VideoListFilterV2Res>(&req,
                                                                  YFFuture<PSS_VideoListFilterV2Res>::CallbackType(),
                                                                  StringConsistentHashLoadBalance::hashKey(uid.sGuid),
                                                                  1);
  f.setTimeoutMs(20);
}

bool AnchorAlgoIf::filterByRpc(const UserId& uid, const std::vector<uint64_t>& vids, std::vector<uint64_t>& filtered)
{
  PSS_VideoListFilterV2 req;
  req.guid = uid;
  req.vids.insert(vids.begin(), vids.end());
  auto f = m_vedioListFilterClient.call<PSS_VideoListFilterV2Res>(&req,
                                                                  YFFuture<PSS_VideoListFilterV2Res>::CallbackType(),
                                                                  StringConsistentHashLoadBalance::hashKey(uid.sGuid),
                                                                  1);

  f.setTimeoutMs(100);
  if (!f.get() || !f.isOk())
  {
    FUNLOG(Info, "fail uid=%s", uid.sGuid.c_str());
    YFMetrics()->incCounter("AnchorAlgoIfError", "filterByRpc", 1, "all");
    return false;
  }

  const PSS_VideoListFilterV2Res & res = f.value();

  for (auto& v : res.filterVids)
    filtered.push_back(v);

  m_statfiltered.rpcFiltered += filtered.size();

  return true;
}

bool AnchorAlgoIf::preFilter(uint64_t vid, std::set<uint64_t>& setVids)
{
  //过滤不在视频池
  auto it = m_videoInfo.find(vid);
  if (it == m_videoInfo.end())
  {
    ++m_statfiltered.notInPool;
    return false;
  }

  //不插入重复的vid
  auto ret = setVids.insert(vid);
  if (ret.second == false)
  {
    ++m_statfiltered.duplicate;
    return false;
  }

  return true;
}

void AnchorAlgoIf::localFilter(const UserId& uid, const std::vector<uint64_t>& vids,
                               std::vector<uint64_t>& filteredVids)
{
  for (auto it = vids.begin(); it != vids.end(); ++it)
  {
    if (isLocalFilter(uid, *it))
      filteredVids.push_back(*it);
  }
}

void AnchorAlgoIf::addLocalFilter(const UserId& uid, vector<uint64_t>& vids)
{
  if (m_localRecommmededVid.count(uid.sGuid) == 0)
    m_localRecommmededVidTs[getSecTime()].insert(uid.sGuid);
  m_localRecommmededVid[uid.sGuid].insert(vids.begin(), vids.end());
}

bool AnchorAlgoIf::isLocalFilter(const UserId& uid, uint64_t vid)
{
  if (m_localRecommmededVid.count(uid.sGuid) != 0)
  {
    if (m_localRecommmededVid[uid.sGuid].count(vid) != 0)
    {
      ++m_statfiltered.localHit;
      return true;
    }
  }

  return false;
}

bool AnchorAlgoIf::isExcludeFilter(uint64_t vid, const std::set<uint64_t>& vids)
{
  if (vids.count(vid) != 0)
  {
    ++m_statfiltered.exclude;
    return true;
  }

  return false;
}

bool AnchorAlgoIf::syncVidInfo()
{
  std::map<uint64_t, VStatInfo> videoInfo;

  // 获得热门视频列表
  bool retry = false;
  uint32_t retryCount = 0;
  std::string hint = "";

  size_t vidSize = 0;
  for (uint64_t index = 0; true; ++index)
  {
    if (retry)
    {
      if (++retryCount >= GETLIST_RETRY_THRESHOLD)
      {
        FUNLOG(Info, "failed to get video info list after %d times retry!", GETLIST_RETRY_THRESHOLD);
        return true;
      }

      YFSleep(GETLIST_RETRY_INTERVAL);
    }
    retry = false;

    FUNLOG(Info, "updating list, index=%ju", index);

    // 获取视频id列表
    PSS_GetVideoList reqList;
    reqList.hint = hint;
    auto futureList = m_videoListClient.call<PSS_GetVideoListRes>(&reqList);

    if (!futureList.get())
    {
      FUNLOG(Info, "failed to get video list!");
      retry = true;
      continue;
    }

    PSS_GetVideoListRes &resList = const_cast<PSS_GetVideoListRes&>(futureList.value());
    auto rescode = static_cast<EGetVideoListRescode>(resList.rescode);
    switch (rescode)
    {
    case E_GETVIDEOLIST_OK:
    case E_GETVIDEOLIST_OK_INC:
      FUNLOG(Info, "video list etrieved, count=%zu", resList.vids.size());
      break;
    case E_GETVIDEOLIST_ERROR:
      FUNLOG(Info, "video list Error occurred!");
      retry = true;
      break;
    default:
      FUNLOG(Info, "video list undefined rescode: %u", static_cast<uint32_t>(rescode));
      retry = true;
      break;
    }

    if (retry)
      continue;

    // 获取视频详细信息
    PSS_GetVideoInfoList reqInfoList;
    reqInfoList.vids.swap(resList.vids);
    auto futureListInfo = m_videoListClient.call<PSS_GetVideoInfoListRes>(&reqInfoList);

    PSS_GetVideoStatPropBatchReq reqPropInfo;
    reqPropInfo.vids.swap(reqInfoList.vids);
    reqPropInfo.props.insert("wch_sq");
    reqPropInfo.props.insert("brw_sq");
    auto futurePropInfo = m_userBehaviorClient.call<PSS_GetVideoStatPropBatchRes>(&reqPropInfo, YFFuture<PSS_GetVideoStatPropBatchRes>::CallbackType(),
                                                                                  StringConsistentHashLoadBalance::hashKey("1"), 1);

    if (!futureListInfo.get() || !futurePropInfo.get())
    {
      FUNLOG(Info, "failed to get video info list!");
      retry = true;
      continue;
    }

    PSS_GetVideoInfoListRes &resListInfo = const_cast<PSS_GetVideoInfoListRes&>(futureListInfo.value());
    for (const auto& vi : resListInfo.vidInfos)
    {
      videoInfo[vi.first] = vi.second;
      videoInfo[vi.first].watchedCnt = 0;
      videoInfo[vi.first].browsedCnt = 0;
    }

    PSS_GetVideoStatPropBatchRes &resPropInfo = const_cast<PSS_GetVideoStatPropBatchRes&>(futurePropInfo.value());
    for (const auto& vp : resPropInfo.vid2props)
    {
      VStatInfo& info = videoInfo[vp.first];
      info.watchedCnt = __getProp(vp.second, "wch_sq");
      info.browsedCnt = __getProp(vp.second, "brw_sq");
    }

    FUNLOG(Info, "video info list segment returned, video#:%zu", resListInfo.vidInfos.size());
    vidSize += resListInfo.vidInfos.size();

    if (rescode == E_GETVIDEOLIST_OK)
    {
      FUNLOG(Info, "finished, video#:%zu/%zu", videoInfo.size(), vidSize);
      break;
    }
    else if (rescode == E_GETVIDEOLIST_OK_INC)
    {
      hint = resList.hint;
      retryCount = 0;
    }
  }

  if (videoInfo.size() < 1000)
  {
    FUNLOG(Error, "failed to get videos! video#<1000");
    YFMetrics()->incCounter("OnlineComputeSvc", "syncVidInfo", 1, "videoSize");
    return true;
  }

  // 成功
  m_videoInfo.swap(videoInfo);

  if (m_firstSyncOwnerInfo == false)
  {
    updateOwnerInfo();
    m_firstSyncOwnerInfo = true;
  }

  m_status = 1;

  return true;
}

bool AnchorAlgoIf::updateOwnerInfo()
{
  std::map<uint64_t, std::vector<uint64_t> > temp;
  std::map<uint64_t, std::list<uint64_t> > scorelist;//ownerid-score
  std::map<uint64_t, std::map<uint64_t, std::vector<uint64_t> > > score2vids;//ownerid-score-vids

  for (auto& v : m_videoInfo)
  {
    VStatInfo& info = v.second;
    if (getSecTime() > info.timestamp + DURING_ONE_DAY * 15)
      continue;
    uint64_t score = 0;
    if (info.browsedCnt > 500)
      score = ((float)info.watchedCnt / (float)info.browsedCnt) * 1000;
    scorelist[info.ownerUid].push_back(score);
    score2vids[info.ownerUid][score].push_back(v.first);
  }

  for (auto& v : scorelist)
  {
    uint64_t ownerId = v.first;
    std::list<uint64_t>& score = v.second;

    score.sort([](const uint64_t& x, const uint64_t& y)
    {
      return x > y;
    });

    for (auto& s : score)
    {
      std::vector<uint64_t>& refVids = score2vids[ownerId][s];
      std::copy(refVids.begin(), refVids.end(), std::back_inserter(temp[ownerId]));
    }
  }

  m_anchorInfo.swap(temp);

  return true;
}

bool AnchorAlgoIf::checkLatestRedis()
{
  for (int i = 1; i <= 2; ++i)
  {
    if (isSimRedisReady(time(NULL) - i * 24 * 3600))
      break;
  }

  m_scoreRedisNotReadyCnt = 0;
  m_simRedisNotReadyCnt = 0;

  return true;
}

bool AnchorAlgoIf::isSimRedisReady(time_t t)
{
  char tmpBuf[128];
  strftime(tmpBuf, 128, "%Y%m%d", localtime(&t));

  ostringstream os;
  os << "sim-" << tmpBuf << "-";
  std::string simTablePrefix = os.str();

  PRedisTableStatusReq simReq(simTablePrefix);
  YFFuture<PRedisTableStatusRes> fm = m_pAlgoSimRedisClient->call<YF_PRedisTableStatus>(&simReq);
  if (fm.get())
  {
    const PRedisTableStatusRes& res = *(fm.getValuePtr());
    if (res.resCode == 0 && res.status == 1)
    {
      if (m_simTablePrefix != simTablePrefix)
        FUNLOG(Info, "sim table updated, prefix %s", simTablePrefix.c_str());
      m_simTablePrefix = simTablePrefix;
      return true;
    }
    else
    {
      //早上7点redis读取status失败，上报到metrics
      time_t now = time(NULL);
      struct tm* local = localtime(&now);
      if (local->tm_hour >= REDIS_STATUS_READY_HOUR)
      {
        ++m_simRedisNotReadyCnt;
        FUNLOG(Error, "sim table not updated in %u:00", local->tm_hour);
      }

      YFMetrics()->incCounter("AnchorAlgoIfError", "simTableNotReady", m_simRedisNotReadyCnt, "all");
    }
  }

  return false;
}

bool AnchorAlgoIf::checkRecommendPool()
{
  std::vector<uint32_t> deleteTs;
  for (auto& v : m_scoreRecommendPoolTs)
  {
    if (getSecTime() > v.first + DURING_ONE_HOUR)
    {
      std::set<std::string>& uids = v.second;
      for (auto& v : uids)
      {
        m_scoreRecommendPool.erase(v);
        m_uid2AnchorList.erase(v);
        FUNLOG(Info, "erase score, uid:%s", v.c_str());
      }
      deleteTs.push_back(v.first);
    }
  }

  for (auto& v : deleteTs)
    m_scoreRecommendPoolTs.erase(v);

  return true;
}

bool AnchorAlgoIf::checkLocalFilter()
{
  std::vector<uint32_t> deleteTs;
  for (auto& v : m_localRecommmededVidTs)
  {
    if (getSecTime() > v.first + DURING_ONE_HOUR)
    {
      std::set<std::string>& uids = v.second;
      for (auto& v : uids)
        m_localRecommmededVid.erase(v);
      deleteTs.push_back(v.first);
    }
  }

  for (auto& v : deleteTs)
    m_localRecommmededVidTs.erase(v);

  return true;
}

bool AnchorAlgoIf::onMetrics()
{
  YFMetrics()->setGauge("AnchorAlgoIf", "total", m_statfiltered.total, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "notInPool", m_statfiltered.notInPool, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "duplicate", m_statfiltered.duplicate, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "version", m_statfiltered.version, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "exclude", m_statfiltered.exclude, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "localHit", m_statfiltered.localHit, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "rpcFiltered", m_statfiltered.rpcFiltered, YFS2SMgr()->getMyServerIdStr());
  YFMetrics()->setGauge("AnchorAlgoIf", "ownerId", m_statfiltered.ownerId, YFS2SMgr()->getMyServerIdStr());

  FUNLOG(Info, "total#%u, notInPool#%u, dup#%u, ver#%u, exclude#%u, localhit#%u, rpc#%u, ownerId#%u",
         m_statfiltered.total, m_statfiltered.notInPool, m_statfiltered.duplicate, m_statfiltered.version,
         m_statfiltered.exclude, m_statfiltered.localHit, m_statfiltered.rpcFiltered, m_statfiltered.ownerId);

  m_statfiltered.total = m_statfiltered.notInPool = m_statfiltered.duplicate = m_statfiltered.version =
    m_statfiltered.exclude = m_statfiltered.localHit = m_statfiltered.rpcFiltered = m_statfiltered.ownerId = 0;

  return true;
}

void AnchorAlgoIf::onUpdate(const ConfigUnit &unit)
{
  if (unit.group != m_configName)
    return;

  FUNLOG(Info, "name = %s, key = %s, value = %s, op = %d",
         unit.group.c_str(), unit.key.c_str(), unit.value.c_str(), unit.op);

  if (unit.key == "m_scoreTopCounter")
  {
    m_scoreTopCounter = fromstring<float>(unit.value);
  }

  if (unit.key == "m_simUsingCounter")
  {
    m_simUsingCounter = fromstring<uint32_t>(unit.value);
  }

  if (unit.key == "m_simScore")
  {
    m_simScore = fromstring<uint32_t>(unit.value);
  }
}