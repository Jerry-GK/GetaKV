lab2A:
Raft�����⣺
����P��һ������������server����������term����leaderһ��Ϊ1ʱ��������һ��server��leaderͨ�ų����ʱ��û�ж�RPC-APPENDENTRIES��Ӧ(���������󲿷ֵ�����ȫ��followerͨ������)��������term�����2����Ϊcandidate�������ʱ�ָ�leader�ָ���ͨ�š�
- ����1: P�����£�candidate��leader�����RPC-REQUESTVOTE��leader���յ���term����������Ƿ���Ҫ���ϱ��follower��ע���ʱleader���Ǹ�����serverͨ�������ģ�candidateֻ�ܸ�һ����ͨ�ţ�������Ӧ����leader������leader��
- ����2: �������1�Ĵ��ǿ϶��ģ���ôleader(����candidate)��Ϊfollower���Ƿ�������Ҫ����candidateͶƱ������reply falseȻ��ƽ��������candidate��ͶƱ����
- ����3: P�����£����candidate�����״̬ת�����������յ���leader֮ǰû���ͳɹ���RPC-APPENDENTRIES����RPC�����term������term�ͣ���ôcandidateӦ����Ϊ����leader���Լ�����ѡ�١����follower(term����)�����Ǿܾ�����reply false��
- ����4: P�����£�������3�У�candidate�϶���ظ�leader��RPC-APPENDENTRIES����ôleader�ڽ��յ����������reply��term���Լ�����ôӦ��Ӧ�ñ��follower(����ʵ������2����)
- ����5: (��P�����޹�)���leader���յ�������һ��leader��RPC-APPENDENTRIES���ᷢ��ʲô��(����һ��leader����һ��ʱ���failure�лظ������ʱ�������ɵ���leader����leader������RPC-APPENDENTRIES)
- ����6: (��P�����޹�)���ѡ���ڼ䷢����split vote��candidate����follower����������¼������·���ѡ�١���ô��candidate���follower��ʱ���費��Ҫ������term�ָ�(���ͣ���һ)�����Ǳ���ԭ����term��

ע��P��������1,2,4��ʵ����ֻ�����ֲ���
- ����1: candidate���ȡ�leader����յ���candidate��RPC������߻ظ����������ϱ��follower����candidateͬ��term��
- ����2: leader���ȡ�leader��Զ�ܾ�ͶƱ���ܾ���candidate�������ظ������follower��leader��candidate���͵�RPC-APPENDENTRIES��ʹtermҪ�ͣ�Ҳ����candidateǿ����leader�����follower��ֻ�е���candidate��ѡ�ɹ������leader������leader����RPC-APPENDENTRIESʱ����leader�Ż���follower��

���վ�������1(ò������Ǵ𰸣���֪��leader���Ȳ�����ʲô����)

- ����2��: leader(��candidate)���follower�󣬱������ϸ��������follower���Ǹ�candidateͶƱ��
- ����5��: ������ԣ����������ֲ��ԣ�����������ܷ������Լ�term�ͣ���ͬ��term�����follower��
- ����6��: ��ѡʧ�ܣ�term����Ҫ���͡���


- ����7: ��Leader������server�㲥AppendEntries����Candidate������server�㲥RequestVoteʱ���Ƿ���Ҫ�ȴ���Щ�㲥ȫ�����ؽ������ʹ���ܴ���RPC��ʱ��
- �𰸣���Ҫ��������ܵ���data race�����ܽ������ܡ�

- ����8: �Ƿ����ͬʱ��������term��ȵ�Leader��
- �𰸣�Raft�в����ܴ���term��ȵ�Leader��Leaderȷʵ�����ж������ֻ��һ�����µġ���Ч��Leader�����Leader�յ�����һ��term��ȵ�Leader������AppendEntries��RPC��������Ϊ�������쳣��

- ����9: �Ƿ��б�Ҫ��ʧ�ܻ�ʱ��RPC����������������ͬһserver�����յ�������ͬ��rpc���󣬻᲻�������⣿