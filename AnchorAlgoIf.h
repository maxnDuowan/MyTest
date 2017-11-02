#pragma once

#include "YF.h"

#include "AnchorRedisInterface.h"
#include "GetAlgoRecommendInterface.h"
#include "VideoListInterface.h"
#include "VideoListFilterInterface.h"
#include "MainConfigWrapper.h"
#include "RsConst.h"

using namespace yycf;
using namespace yycf::rpc;
using namespace protocol::yc_rcm;
using namespace config;

struct filteredStat
{
  uint32_t total;
  uint32_t notInPool;
  uint32_t duplicate;
  uint32_t version;
  uint32_t exclude;
  uint32_t localHit;
  uint32_t rpcFiltered;
  uint32_t ownerId;
};
  
class AnchorAlgoIf : public IConfigWatcher
{
public:
  AnchorAlgoIf(int argc, char **argv);

  uint16_t onGetRecommendListByGuid(PSS_GetAlgoRecommendVideoListV2 * pReq, PSS_GetAlgoRecommendVideoListV2Res * pRes, const ConnInfo & connInfo);
  uint16_t onGetRecommendListByVid(PSS_GetItemCFRecommendVidsByVid * pReq, PSS_GetItemCFRecommendVidsByVidRes * pRes, const ConnInfo & connInfo);

  virtual void onUpdate(const ConfigUnit &unit);

  bool isReady() { return (m_status == 1); }

private:
  uint32_t buildSimPool(const UserId& uid, const std::set<uint64_t>& excludeVids);
  bool selectVid(const UserId& uid, uint32_t reqCounter, const std::set<uint64_t>& excludeVids, std::vector<uint64_t>& vids, std::string& output);
  bool getAnchorsScore(const UserId& uid, std::vector<uint64_t>& vids);
  bool getSimAnchors(uint64_t vid, std::vector<uint64_t>& simAnchors);
  uint64_t getAnchorId(uint64_t vid);

  void localFilter(const UserId& uid, const std::vector<uint64_t>& vids, std::vector<uint64_t>& filteredVids);
  void addLocalFilter(const UserId& uid, vector<uint64_t>& vids);
  bool isLocalFilter(const UserId& uid, uint64_t vid);
  bool filterByRpc(const UserId& uid, const std::vector<uint64_t>& vids, std::vector<uint64_t>& filtered);
  void preFilterByRpc(const UserId& uid);
  bool preFilter(uint64_t vid, std::set<uint64_t>& setVids);
  bool isExcludeFilter(uint64_t vid, const std::set<uint64_t>& vids);

  bool syncVidInfo();
  bool updateOwnerInfo();
  bool isSimRedisReady(time_t t);
  bool checkRecommendPool();
  bool checkLocalFilter();
  bool checkLatestRedis();
  bool onMetrics();

private:
  std::string m_rcProxyS2SName;
  std::string m_filterS2SName;
  std::string m_videoListS2SName;
  std::string m_configName;

  uint32_t m_scoreTopCounter;       //ȡscore top���ƶ�vid������
  uint32_t m_simUsingCounter;       //ÿ���������Ѽ�����������������
  uint32_t m_simScore;              //�������ƶ���ֵ

  std::string m_simTablePrefix;     //�û����ƶ�redisǰ׺
  std::string m_scoreTablePrefix;   //�û�����redisǰ׺

  uint32_t m_status;                //����״̬��1��ʾ���ò�ע��s2s
  filteredStat m_statfiltered;      //����ͳ��
  uint32_t m_scoreRedisNotReadyCnt;
  uint32_t m_simRedisNotReadyCnt;
  bool m_firstSyncOwnerInfo;

  YFSameGroupHashYYMsgClient m_userBehaviorClient;
  YFSameGroupHashYYMsgClient m_vedioListFilterClient;
  YFYYMsgClient m_videoListClient;
  YFSharedPtr<YFRedisRpcClient> m_pAlgoScoreRedisClient;
  YFSharedPtr<YFRedisRpcClient> m_pAlgoSimRedisClient;

  std::map<uint64_t, VStatInfo> m_videoInfo; //ȫ����Ƶ����

  std::map<std::string, std::set<uint64_t> > m_localRecommmededVid;//�����û����Ƽ���
  std::map<uint32_t, std::set<std::string> > m_localRecommmededVidTs;//�����û����Ƽ����ӵĴ���ʱ��

  std::map<std::string, std::map<uint64_t, std::vector<uint64_t> > > m_scoreRecommendPool;//�û�score�Ƽ����ӣ�uid-ownerid-vector<vid>, vid����similarityRadio����
  std::map<uint32_t, std::set<std::string> > m_scoreRecommendPoolTs;//�����û�vid score�Ƽ����ӵĴ���ʱ��
  std::map<std::string, std::list<uint64_t> > m_uid2AnchorList;//uid-list<anchor>

  std::map<uint64_t, std::vector<uint64_t> > m_anchorInfo;//�����Ƽ���

  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::syncVidInfo> m_updateVidInfoTimer;
  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::updateOwnerInfo> m_updateOwnerInfoTimer;
  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::checkRecommendPool> m_checkRecommendTimer;
  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::checkLocalFilter> m_checkRecommededVidTimer;
  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::checkLatestRedis> m_checkRedisStatusTimer;
  YFTimerHandler<AnchorAlgoIf, &AnchorAlgoIf::onMetrics> m_onMetricsTimer;
};