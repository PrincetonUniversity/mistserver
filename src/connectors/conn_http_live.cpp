/// \file conn_http_dynamic.cpp
/// Contains the main code for the HTTP Dynamic Connector

#include <iostream>
#include <iomanip>
#include <sstream>
#include <queue>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <getopt.h>
#include <mist/socket.h>
#include <mist/http_parser.h>
#include <mist/json.h>
#include <mist/dtsc.h>
#include <mist/mp4.h>
#include <mist/config.h>
#include <sstream>
#include <mist/stream.h>
#include <mist/timing.h>
#include <mist/ts_packet.h>


//sharva_modn
#include <fstream>
#define __NR_map_back 318
#define __NR_vnet_yank 319
#define __NR_vnet_yankputdata 320
#define __NR_vnet_yankdata 321

/// Holds everything unique to HTTP Connectors.
namespace Connector_HTTP {
///\brief Builds an index file for HTTP Live streaming.
///\param metadata The current metadata, used to generate the index.
///\return The index file for HTTP Live Streaming.
std::string liveIndex(JSON::Value & metadata){
	std::stringstream Result;
	if ( !metadata.isMember("live")){
		int longestFragment = 0;
		for (JSON::ArrIter ai = metadata["frags"].ArrBegin(); ai != metadata["frags"].ArrEnd(); ai++){
			if ((*ai)["dur"].asInt() > longestFragment){
				longestFragment = (*ai)["dur"].asInt();
			}
		}
		Result << "#EXTM3U\r\n"
				"#EXT-X-TARGETDURATION:" << (2*longestFragment / 1000) + 1 << "\r\n"
				"#EXT-X-MEDIA-SEQUENCE:0\r\n";
		for (JSON::ArrIter ai = metadata["frags"].ArrBegin(); ai != metadata["frags"].ArrEnd(); ai++){
			int num =   (*ai)["num"].asInt();
			int dur = (*ai)["dur"].asInt() / 1000;
			int len = (*ai)["len"].asInt();


			// if(ai != metadata["frags"].ArrBegin() && ai != metadata["frags"].ArrBegin()+1){
			ai++;
			if(ai != metadata["frags"].ArrEnd()){
				dur += (*ai)["dur"].asInt() / 1000;
				len += (*ai)["len"].asInt()-1;

				*ai++;
						if(ai != metadata["frags"].ArrEnd()){
							dur += (*ai)["dur"].asInt() / 1000;
							len += (*ai)["len"].asInt()-1;
							ai++;
							if(ai != metadata["frags"].ArrEnd()){
								dur += (*ai)["dur"].asInt() / 1000;
								len += (*ai)["len"].asInt()-1;
							}
						}
			}
			//}

			//sharva_mod
			Result << "#EXTINF:" << dur << ", no desc\r\n" << num << "_" << len << ".ts\r\n";
			//Result << "#EXT-X-DISCONTINUITY\r\n#EXTINF:" << dur << ", no desc\r\n" << num << "_" << len << ".ts\r\n";

			if(ai == metadata["frags"].ArrEnd())
				break;
		}
		Result << "#EXT-X-ENDLIST";
	}else{
		if (metadata["missed_frags"].asInt() < 0){
			metadata["missed_frags"] = 0ll;
		}
		Result << "#EXTM3U\r\n"
				"#EXT-X-MEDIA-SEQUENCE:" << metadata["missed_frags"].asInt() <<"\r\n"
				"#EXT-X-TARGETDURATION:30\r\n";
		for (JSON::ArrIter ai = metadata["frags"].ArrBegin(); ai != metadata["frags"].ArrEnd(); ai++){
			int num =   (*ai)["num"].asInt();
			int dur = (*ai)["dur"].asInt() / 1000;
			int len = (*ai)["len"].asInt();
			ai++;
			if(ai != metadata["frags"].ArrEnd()){
				dur += (*ai)["dur"].asInt() / 1000;
				len += (*ai)["len"].asInt();
			}
			//sharva_mod
			Result << "#EXTINF:" << dur << ", no desc\r\n" << num << "_" << len << ".ts\r\n";
			//Result << "#EXT-X-DISCONTINUITY\r\n#EXTINF:" << dur << ", no desc\r\n" << num << "_" << len << ".ts\r\n";

			if(ai == metadata["frags"].ArrEnd())
				break;
		}
	}
#if DEBUG >= 8
	std::cerr << "Sending this index:" << std::endl << Result.str() << std::endl;
#endif
	return Result.str();
} //liveIndex

///\brief Main function for the HTTP Live Connector
///\param conn A socket describing the connection the client.
///\return The exit code of the connector.
int liveConnector(Socket::Connection conn){
	std::stringstream TSBuf;
	long long int TSBufTime = 0;

	DTSC::Stream Strm; //Incoming stream buffer.
	HTTP::Parser HTTP_R, HTTP_S; //HTTP Receiver en HTTP Sender.

	bool ready4data = false; //Set to true when streaming is to begin.
	Socket::Connection ss( -1);
	std::string streamname;
	std::string recBuffer = "";

	TS::Packet PackData;
	int PacketNumber = 0;
	long long unsigned int TimeStamp = 0;
	int ThisNaluSize;
	char VideoCounter = 0;
	char AudioCounter = 0;
	bool IsKeyFrame;
	MP4::AVCC avccbox;
	bool haveAvcc = false;

	std::vector<int> fragIndices;

	std::string manifestType;

	int Segment = -1;
	int temp;
	unsigned int lastStats = 0;
	conn.setBlocking(false); //do not block on conn.spool() when no data is available

	//////////////
	int num_streams = 2;
	bool meta_pending  = true;
	//int sizes[] = {78734,42419,21622,16270}; 
	//int bitrate_arr[] = {131,70,36,27}; //KBps
	int bitrate_arr[] = {131,29}; 
	int curr_bitrate_index = 0; 
	//sharva_modn
	int last_iframes[50];
	int iframe_boundaries[50], last_size = 0, last_header_size = 0;
	int curr_seg, total_seg, i_seg=0;
	int pending_frames = 0, sending_segment = 0;
	int done = 0;
	int last_window, last_rtt = 0;
	int miss_count = 0; //no. of times the performance drops
	//////////////


	while (conn.connected()){
		if (conn.spool() || conn.Received().size()){
			//make sure it ends in a \n
			if ( *(conn.Received().get().rbegin()) != '\n'){
				std::string tmp = conn.Received().get();
				conn.Received().get().clear();
				if (conn.Received().size()){
					conn.Received().get().insert(0, tmp);
				}else{
					conn.Received().append(tmp);
				}
			}
			if (HTTP_R.Read(conn.Received().get())){
#if DEBUG >= 5
				std::cout << "Received request: " << HTTP_R.getUrl() << std::endl;
#endif
				conn.setHost(HTTP_R.GetHeader("X-Origin"));
				streamname = HTTP_R.GetHeader("X-Stream");
				if ( !ss){
					ss = Util::Stream::getStream(streamname);
					if ( !ss.connected()){
#if DEBUG >= 1
						fprintf(stderr, "Could not connect to server!\n");
#endif
						HTTP_S.Clean();
						HTTP_S.SetBody("No such stream is available on the system. Please try again.\n");
						conn.SendNow(HTTP_S.BuildResponse("404", "Not found"));
						ready4data = false;
						continue;
					}
					ss.setBlocking(false);

					//make sure metadata is received
					meta_pending = true;
					while ( meta_pending && ss.connected()){
						if (ss.spool()){
							while (Strm.parsePacket(ss.Received())){
								//do nothing
							}
						}

						meta_pending = false;

						for(int i=0;i<num_streams;i++)
							if(!Strm.metadata_arr[i])
								meta_pending = true;
					}
				}
				if (HTTP_R.url.find(".m3u") == std::string::npos){
					temp = HTTP_R.url.find("/", 5) + 1;
					Segment = atoi(HTTP_R.url.substr(temp, HTTP_R.url.find("_", temp) - temp).c_str());
					temp = HTTP_R.url.find("_", temp) + 1;
					int frameCount = atoi(HTTP_R.url.substr(temp, HTTP_R.url.find(".ts", temp) - temp).c_str());
					if (Strm.metadata.isMember("live")){
						int seekable = Strm.canSeekFrame(Segment);
						if (seekable < 0){
							HTTP_S.Clean();
							HTTP_S.SetBody("The requested fragment is no longer kept in memory on the server and cannot be served.\n");
							conn.SendNow(HTTP_S.BuildResponse("412", "Fragment out of range"));
							HTTP_R.Clean(); //clean for any possible next requests
							std::cout << "Fragment @ F" << Segment << " too old (F" << Strm.metadata_arr[curr_bitrate_index]["keynum"][0u].asInt() << " - " << Strm.metadata_arr[curr_bitrate_index]["keynum"][Strm.metadata_arr[curr_bitrate_index]["keynum"].size() - 1].asInt() << ")" << std::endl;
							continue;
						}
						if (seekable > 0){
							HTTP_S.Clean();
							HTTP_S.SetBody("Proxy, re-request this in a second or two.\n");
							conn.SendNow(HTTP_S.BuildResponse("208", "Ask again later"));
							HTTP_R.Clean(); //clean for any possible next requests
							std::cout << "Fragment @ F" << Segment << " not available yet (F" << Strm.metadata_arr[curr_bitrate_index]["keynum"][0u].asInt() << " - " << Strm.metadata_arr[curr_bitrate_index]["keynum"][Strm.metadata["keynum"].size() - 1].asInt() << ")" << std::endl;
							continue;
						}
					}
					std::stringstream sstream;

					haveAvcc = false;

					std::cerr << "Created new connection11 " << std::endl;
					//std::cerr<<Segment<<"sent\n";

					//std::cerr<<"Segment ="<<Segment<<"HTTP_R.url"<<HTTP_R.url<<"\n"; 
					///////////
					munmap ( conn.stats, MAP_SHARED );
					void *ptr1 = mmap (NULL, MAPPED, PROT_WRITE | PROT_READ, MAP_SHARED, conn.sock, 0);
					conn.stats = ( struct tcp_sock_stats * ) ptr1;

					if(ptr1==(void *)-1)
					{
						std::cerr<<"the memory map failed here\n";
						return 0;
					}

					//std::cerr << " suspected point3 \n";
					
					
					int bandwidth = (conn.stats->snd_cwnd*1000)/conn.stats->srtt ;// 2/*scaling heuristic*/;

					curr_bitrate_index = num_streams-1;
					for(int i=0;i<num_streams;i++)
					{
						if(bitrate_arr[i]<bandwidth)
						{
							curr_bitrate_index = i;
							break;
						}

					}


					//sharva_mod, hack to get it work
					if(conn.stats->snd_cwnd==14480)
						curr_bitrate_index = 0;

					std::cerr<<" cwnd="<<conn.stats->snd_cwnd<<" bandwidth="<<bandwidth<<"curr_bitrate_index="<<curr_bitrate_index<<"\n";

					//curr_bitrate_index = 0;

					Segment -= pending_frames;
					frameCount += pending_frames;
					pending_frames = 0;


					std::cerr<<"Segment ="<<Segment<<"\n";

					sstream << "g " << Segment << " " << curr_bitrate_index << "\n";
					//if(Segment==1)
					//Segment = 0;
					//sstream << "f " << Segment << "\n";
					//sharva_modn
					i_seg = 0;
					curr_seg = Segment;
					total_seg = frameCount-1;
					sending_segment = 1;
					///////////


					for (int i = 0; i < frameCount-1; i++){
						sstream << "o \n";
					}


					ss.SendNow(sstream.str().c_str());


				}else{
					if (HTTP_R.url.find(".m3u8") != std::string::npos){
						manifestType = "audio/x-mpegurl";
					}else{
						manifestType = "audio/mpegurl";
					}
					HTTP_S.Clean();
					HTTP_S.SetHeader("Content-Type", manifestType);
					HTTP_S.SetHeader("Cache-Control", "no-cache");
					std::string manifest = liveIndex(Strm.metadata);
					HTTP_S.SetBody(manifest);
					conn.SendNow(HTTP_S.BuildResponse("200", "OK"));
				}
				ready4data = true;
				HTTP_R.Clean(); //clean for any possible next requests
			}
		}else{
			Util::sleep(1);
		}

		//curr_seg = 0;
		//std::cerr<<curr_seg;
		if (ready4data){
			unsigned int now = Util::epoch();
			if (now != lastStats){
				lastStats = now;
				ss.SendNow(conn.getStats("HTTP_Live").c_str());
			}
			if (ss.spool()){
				while (Strm.parsePacket(ss.Received())){

					if (Strm.lastType() == DTSC::PAUSEMARK){
						TSBuf.flush();
						if (TSBuf.str().size()){
							HTTP_S.Clean();
							HTTP_S.protocol = "HTTP/1.1";
							HTTP_S.SetHeader("Content-Type", "video/mp2t");
							HTTP_S.SetHeader("Connection", "keep-alive");
							HTTP_S.SetBody("");
							HTTP_S.SetHeader("Content-Length", TSBuf.str().size());
							conn.SendNow(HTTP_S.BuildResponse("200", "OK"));
							conn.SendNow(TSBuf.str().c_str(), TSBuf.str().size());

							//////////////////////////////
							//std::cerr<<"wrote : "<<TSBuf.str().size()<<"\n";
							last_header_size = HTTP_S.BuildResponse("200", "OK").size();;
							last_size = TSBuf.str().size();
							sending_segment = 0;
							last_window = 0;
							last_rtt = 0;
							miss_count = 0;
							//////////////////////////////

							TSBuf.str("");
							PacketNumber = 0;


						}
						TSBuf.str("");
					}
					if ( !haveAvcc){
						avccbox.setPayload(Strm.metadata_arr[curr_bitrate_index]["video"]["init"].asString());
						haveAvcc = true;
					}
					if (Strm.lastType() == DTSC::VIDEO || Strm.lastType() == DTSC::AUDIO){
						Socket::Buffer ToPack;


						//write PAT and PMT TS packets
						if (PacketNumber == 0){
							PackData.DefaultPAT();
							TSBuf.write(PackData.ToString(), 188);
							PackData.DefaultPMT();
							TSBuf.write(PackData.ToString(), 188);
							PacketNumber += 2;
						}

						int PIDno = 0;
						char * ContCounter = 0;
						if (Strm.lastType() == DTSC::VIDEO){
							IsKeyFrame = Strm.getPacket(0).isMember("keyframe");
							if (IsKeyFrame){
								std::cerr<<"keyframe\n";
								iframe_boundaries[i_seg] = TSBuf.str().size();
								std::cerr<<"iframe_boundaries["<<i_seg<<"]"<<iframe_boundaries[i_seg]<<"\n";
								i_seg++;
								TimeStamp = (Strm.getPacket(0)["time"].asInt() * 27000);
							}
							ToPack.append(avccbox.asAnnexB());
							while (Strm.lastData().size()){
								ThisNaluSize = (Strm.lastData()[0] << 24) + (Strm.lastData()[1] << 16) + (Strm.lastData()[2] << 8) + Strm.lastData()[3];
								Strm.lastData().replace(0, 4, TS::NalHeader, 4);
								if (ThisNaluSize + 4 == Strm.lastData().size()){
									ToPack.append(Strm.lastData());
									break;
								}else{
									ToPack.append(Strm.lastData().c_str(), ThisNaluSize + 4);
									Strm.lastData().erase(0, ThisNaluSize + 4);
								}
							}
							ToPack.prepend(TS::Packet::getPESVideoLeadIn(0ul, Strm.getPacket(0)["time"].asInt() * 90));
							PIDno = 0x100;
							ContCounter = &VideoCounter;
						}
						else if (Strm.lastType() == DTSC::AUDIO){
							ToPack.append(TS::GetAudioHeader(Strm.lastData().size(), Strm.metadata_arr[curr_bitrate_index]["audio"]["init"].asString()));
							ToPack.append(Strm.lastData());
							ToPack.prepend(TS::Packet::getPESAudioLeadIn(ToPack.bytes(1073741824ul), Strm.getPacket(0)["time"].asInt() * 90));
							PIDno = 0x101;
							ContCounter = &AudioCounter;
						}

						//initial packet
						PackData.Clear();
						PackData.PID(PIDno);
						PackData.ContinuityCounter(( *ContCounter)++);
						PackData.UnitStart(1);
						if (IsKeyFrame){
							PackData.RandomAccess(1);
							PackData.PCR(TimeStamp);
						}
						unsigned int toSend = PackData.AddStuffing(ToPack.bytes(184));
						std::string gonnaSend = ToPack.remove(toSend);
						PackData.FillFree(gonnaSend);
						TSBuf.write(PackData.ToString(), 188);
						PacketNumber++;

						//rest of packets
						while (ToPack.size()){
							PackData.Clear();
							PackData.PID(PIDno);
							PackData.ContinuityCounter(( *ContCounter)++);
							toSend = PackData.AddStuffing(ToPack.bytes(184));
							gonnaSend = ToPack.remove(toSend);
							PackData.FillFree(gonnaSend);
							TSBuf.write(PackData.ToString(), 188);
							PacketNumber++;
						}


						//if(IsKeyFrame){

						//iframe_boundaries[i_seg] = TSBuf.str().size();
						//std::cerr<<"iframe_boundaries["<<i_seg<<"]"<<iframe_boundaries[i_seg]<<"\n";
						//i_seg++;
						//}


					}
				}
			}
			if ( !ss.connected()){
				break;
			}

			if(sending_segment == 0){
				//std::cerr << " suspected point0 \n";
				
				munmap ( conn.stats, MAP_SHARED );
				void *ptr1 = mmap (NULL, MAPPED, PROT_WRITE | PROT_READ, MAP_SHARED, conn.sock, 0);
				conn.stats = ( struct tcp_sock_stats * ) ptr1;
				
				if(ptr1==(void *)-1)
					return 0;
				
				//std::cerr << " suspected point2 \n";
				int window = conn.stats->snd_cwnd;
				int rtt = conn.stats->srtt;

				//if(miss_count>2)
				//std::cerr << "miss_count ="<<miss_count<<"\n";

				//if(rtt!=0)
				//std::cerr << "bandwidth ="<<(window*1000/rtt);
				
				//if( ((last_window > window) || (last_rtt<rtt) ) && Segment >1)
				if(rtt!=0)
					//if( ((last_window/last_rtt > window/rtt)  ) && Segment >1)
					
					if((window*1000/rtt)  /* *10 scaling as heuristic*/ < bitrate_arr[curr_bitrate_index] && Segment >1)
					{	
						if(last_rtt!=0 && window!=last_window && rtt!=last_rtt)
							miss_count++;

						//std::cerr << "bandwidth ="<<(window*1000/rtt);
						//std::cerr << "last window was greater\n";
						//std::cerr<<" last_window ="<< last_window<<" last_rtt ="<<last_rtt<<" window ="<<window<<" rtt ="<<rtt<<"\n";
					}

				last_window = window;
				last_rtt = rtt;
			}
			
			//in case of 3 fluctuations, do yank
			if (miss_count>10 && sending_segment == 0 && curr_bitrate_index<num_streams-1) 
			//if	( sending_segment == 0 && curr_seg >= 3 && done == 0)
			{

				//std::cerr << " suspected point4 \n";
				std::ofstream myfile  ("/home/sharvanath/mistserver/mistserver_back/debug/live.txt",std::ios::app);
				//myfile<<"starting in live\n";
				//
				syscall(__NR_vnet_yank, conn.sock, 0, -1);	//ask to check the maximum amount yankable
				int yankable = syscall(__NR_vnet_yank, conn.sock, 2, -1);

				//std::cerr << " suspected point6 \n";
				
				//myfile<<"the amount yankable1 is="<<yankable<<"\n";
				std::cerr<<"the amount yankable1 is="<<yankable<<"\n";

				while(yankable<0)
					yankable = syscall(__NR_vnet_yank, conn.sock, 2, -1);

				//myfile<<"the amount yankable is="<<yankable<<"\n";
				std::cerr<<"the amount yankable is="<<yankable<<"\n";

				if(yankable>=0){
					//myfile<<"the amount yankable is="<<yankable<<"\n";
					std::cerr<<"the amount yankable is="<<yankable<<"\n";


					int i=0;


					//myfile<<"last_size ="<<last_size<<" "<<last_header_size<<"\n";
					std::cerr<<"last_size ="<<last_size<<" "<<last_header_size<<"\n";

					for(i=0;i<total_seg;i++){
						std::cerr<<i<<" "<<iframe_boundaries[i]<<"\n";
						if(yankable>=last_size - iframe_boundaries[i] - last_header_size)
						{

							break;
						}
					}

					if(i!=total_seg)
					{
						pending_frames = total_seg - i;
						int to_yank = last_size - iframe_boundaries[i] - last_header_size;


						//tell it to perform a yank of the hundred bytes.
						int yank_asked = syscall(__NR_vnet_yank, conn.sock, 3, to_yank);

						//myfile<<"the amount yank_req is="<<yank_asked<<" "<<to_yank<<"\n";
						std::cerr<<"the amount yank_req is="<<yank_asked<<" "<<to_yank<<"\n";
					}
					//char * buff = new char[to_yank];

					//do
					//{
					//	yankable = syscall(__NR_vnet_yankdata, conn.sock, buff, yank_asked);
					//}
					//while(yankable==-2);
					//replace busy waiting by events



					//std::cerr<<"the first three bytes of yanked data is:"<<buff[0]<<","<<buff[1]<<","<<buff[2]<<"\n";

					//while(1);
					//if the check results are out, do_yank 

					//clear yank
					//syscall(__NR_vnet_yank, conn.sock, 4, 0);
					int is_active = 0;
					do {

						is_active  = syscall(__NR_vnet_yank, conn.sock, 5, -1);
					}
					while(is_active);
				}
				done  = 1;
				sending_segment = 1;
				myfile.close();
			}
		}
	}
	conn.close();
	ss.SendNow(conn.getStats("HTTP_Live").c_str());
	ss.close();
#if DEBUG >= 5
	fprintf(stderr, "HLS: User %i disconnected.\n", conn.getSocket());
#endif
	return 0;
} //HLS_Connector main function

} //Connector_HTTP namespace

///\brief The standard process-spawning main function.
int main(int argc, char ** argv){
	Util::Config conf(argv[0], PACKAGE_VERSION);
	conf.addConnectorOptions(1935);
	conf.parseArgs(argc, argv);
	Socket::Server server_socket = Socket::Server("/tmp/mist/http_live");
	if ( !server_socket.connected()){
		return 1;
	}
	conf.activate();


	while (server_socket.connected() && conf.is_active){
		Socket::Connection S = server_socket.accept();
		if (S.connected()){ //check if the new connection is valid
			pid_t myid = fork();
			if (myid == 0){ //if new child, start MAINHANDLER
				return Connector_HTTP::liveConnector(S);
			}else{ //otherwise, do nothing or output debugging text
#if DEBUG >= 5
				fprintf(stderr, "Spawned new process %i for socket %i\n", (int)myid, S.getSocket());
#endif
			}
		}
	} //while connected
	server_socket.close();
	return 0;
} //main
