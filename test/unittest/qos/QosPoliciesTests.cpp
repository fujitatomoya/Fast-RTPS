// Copyright 2017 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <fastrtps/log/Log.h>
#include <fastrtps/utils/IPLocator.h>
#include <fastrtps/participant/Participant.h>
#include <fastrtps/attributes/ParticipantAttributes.h>
#include <fastrtps/attributes/SubscriberAttributes.h>
#include <fastrtps/subscriber/Subscriber.h>
#include <fastrtps/attributes/PublisherAttributes.h>
#include <fastrtps/publisher/Publisher.h>
#include <fastrtps/Domain.h>

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>

using namespace eprosima::fastrtps;
using namespace eprosima::fastrtps::rtps;

class LatencyBudgetQosTests : public ::testing::Test
{
  public:
    LatencyBudgetQosTests()
    {
        class SubListener:public eprosima::fastrtps::SubscriberListener
        {
        public:
            SubListener():n_matched(0),n_samples(0){};
            ~SubListener(){};
            void onSubscriptionMatched(eprosima::fastrtps::Subscriber* sub, eprosima::fastrtps::rtps::MatchingInfo& info);
            void onNewDataMessage(eprosima::fastrtps::Subscriber* sub);
            int n_matched;
            uint32_t n_samples;
        }sub_listener;
        class PubListener:public eprosima::fastrtps::PublisherListener
        {
        public:
            PubListener():n_matched(0),firstConnected(false){};
            ~PubListener(){};
            void onPublicationMatched(eprosima::fastrtps::Publisher* pub, eprosima::fastrtps::rtps::MatchingInfo& info);
            int n_matched;
            bool firstConnected;
        }pub_listener;
    }

    ~LatencyBudgetQosTests()
    {
    }
};

TEST_F(LatencyBudgetQosTests, DurationCheck)
{

    ParticipantAttributes part_atts;
    part_atts.rtps.setName("Participant_pub");
    Participant* mp_participant = Domain::createParticipant(part_atts);
    
    PublisherAttributes pub_atts;
    pub_atts.topic.topicKind = NO_KEY;
    pub_atts.topic.topicDataType = "HelloWorld";
    pub_atts.topic.topicName = "HelloWorldTopic";
    pub_atts.topic.historyQos.kind = KEEP_LAST_HISTORY_QOS;
    pub_atts.topic.historyQos.depth = 30;
    pub_atts.topic.resourceLimitsQos.max_samples = 50;
    pub_atts.topic.resourceLimitsQos.allocated_samples = 20;
    pub_atts.times.heartbeatPeriod.seconds = 2;
    pub_atts.times.heartbeatPeriod.nanosec = 200*1000*1000;
    pub_atts.qos.m_reliability.kind = RELIABLE_RELIABILITY_QOS;
    pub_atts.qos.m_latencyBudget.duration.seconds = 10;
    Publisher* mp_publisher = Domain::createPublisher(mp_participant,pub_atts,(PublisherListener*)&pub_listener);

    SubscriberAttributes sub_atts;
    sub_atts.topic.topicKind = NO_KEY;
    sub_atts.topic.topicDataType = "HelloWorld";
    sub_atts.topic.topicName = "HelloWorldTopic";
    sub_atts.topic.historyQos.kind = KEEP_LAST_HISTORY_QOS;
    sub_atts.topic.historyQos.depth = 30;
    sub_atts.topic.resourceLimitsQos.max_samples = 50;
    sub_atts.topic.resourceLimitsQos.allocated_samples = 20;
    sub_atts.qos.m_reliability.kind = RELIABLE_RELIABILITY_QOS;
    sub_atts.qos.m_durability.kind = TRANSIENT_LOCAL_DURABILITY_QOS;
    sub_atts.qos.m_latencyBudget.duration.seconds = 20;
    Subscriber* mp_subscriber = Domain::createSubscriber(mp_participant,sub_atts,(SubscriberListener*)&sub_listener);

    EXPECT_EQ(mp_publisher->mp_impl->m_atts.qos.m_latencyBudget.duration.seconds, 10);
    EXPECT_EQ(mp_subscriber->mp_impl->m_atts.qos.m_latencyBudget.duration.seconds, 20);
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
