/// \file player.cpp
/// Holds all code for the MistPlayer application used for VoD streams.

#include <iostream>//for std::cerr
//sharva_mod
#include <fstream>

#include <stdio.h> //for fileno
#include <stdlib.h> //for atoi
#include <sys/time.h>
#include <mist/dtsc.h>
#include <mist/json.h>
#include <mist/config.h>
#include <mist/socket.h>
#include <mist/timing.h>
#include <mist/procs.h>


#include <time.h> 

//under cygwin, recv blocks for ~15ms if no data is available.
//This is a hack to keep performance decent with that bug present.
#ifdef __CYGWIN__
#define CYG_DEFI int cyg_count;
#define CYG_INCR cyg_count++;
#define CYG_LOOP (cyg_count % 20 == 0) &&
#else
#define CYG_DEFI 
#define CYG_INCR 
#define CYG_LOOP 
#endif

///Converts a stats line to up, down, host, connector and conntime values.
class Stats{
public:
	unsigned int up;///<The amount of bytes sent upstream.
	unsigned int down;///<The amount of bytes received downstream.
	std::string host;///<The connected host.
	std::string connector;///<The connector the user is connected with.
	unsigned int conntime;///<The amount of time the user is connected.
	///\brief Default stats constructor.
	///
	///Should not be used.
	Stats(){
		up = 0;
		down = 0;
		conntime = 0;
	}
	;
	///\brief Stats constructor reading a string.
	///
	///Reads a stats string and parses it to the internal representation.
	///\param s The string of stats.
	Stats(std::string s){
		size_t f = s.find(' ');
		if (f != std::string::npos){
			host = s.substr(0, f);
			s.erase(0, f + 1);
		}
		f = s.find(' ');
		if (f != std::string::npos){
			connector = s.substr(0, f);
			s.erase(0, f + 1);
		}
		f = s.find(' ');
		if (f != std::string::npos){
			conntime = atoi(s.substr(0, f).c_str());
			s.erase(0, f + 1);
		}
		f = s.find(' ');
		if (f != std::string::npos){
			up = atoi(s.substr(0, f).c_str());
			s.erase(0, f + 1);
			down = atoi(s.c_str());
		}
	}
};

int main(int argc, char** argv){

	int last_carry=0;

	int num_streams = 2,curr_stream_index=0;

	bool stream_res = true;

	Util::Config conf(argv[0], PACKAGE_VERSION);
	conf.addOption("filename", JSON::fromString("{\"arg_num\":1, \"help\":\"Name of the file to write to stdout.\"}"));
	conf.parseArgs(argc, argv);
	conf.activate();
	int playing = 0;

	Socket::Connection in_out = Socket::Connection(fileno(stdout), fileno(stdin));

	//DTSC::File source = DTSC::File(conf.getString("filename"));

	//sharva_mod
	//std::cout<<conf.getString("filename");

	//sharva_mod3
	std::string streamname = conf.getString("filename").substr(0,conf.getString("filename").find("_"));

	if(streamname.find("_")!= std::string::npos)
	{
		std::ofstream myfile  ("/home/sharvanath/mistserver/mistserver_back/debug/error_log",std::ios::app);
		myfile << "The path name had an _ which is not handled\n, player.cpp";
		myfile.close();
		return 1;
	}


	DTSC::File * source1[num_streams];

	//DTSC::File source = DTSC::File(conf.getString("filename"));
	//source1[0] = DTSC::File(conf.getString("filename"));
	for(int i=0;i<num_streams;i++){
		std::ostringstream ss;
		ss <<streamname<<i<<".dtsc";

		std::cerr << "Here Iam" <<ss.str()<<"\n";
		source1[i] = new DTSC::File(ss.str());
	}

	//DTSC::File 
	//source1[1] = DTSC::File("/home/sharvanath/mistserver/big2.dtsc");

	int src1=1;


	JSON::Value meta1[num_streams];
	for(int i=0;i<num_streams;i++)
	{	
		meta1[i] = source1[i]->getMeta();
		//send the header
		std::string meta_str = meta1[i].toNetPacked();
		in_out.Send(meta_str);
	}

	//
	//meta1[0] = source.getMeta();
	//source1[0].getMeta();
	//send the header
	//std::string meta_str = meta1[0].toNetPacked();
	//in_out.Send(meta_str);

	JSON::Value meta = meta1[0];
	//meta1[1] = source1[1].getMeta();
	//meta_str = meta1[1].toNetPacked();
	//in_out.Send(meta_str);


	if ( !(meta.isMember("keytime") && meta.isMember("keybpos") && meta.isMember("keynum") && meta.isMember("keylen") && meta.isMember("frags"))
			&& meta.isMember("video")){
		//file needs to be DTSCFix'ed! Run MistDTSCFix executable on it first
		std::cerr << "Calculating / writing / updating VoD metadata..." << std::endl;
		Util::Procs::Start("Fixer", Util::getMyPath() + "MistDTSCFix " + conf.getString("filename"));
		while (Util::Procs::isActive("Fixer")){
			Util::sleep(5000);
		}
		std::cerr << "Done! Aborting this request to make sure all goes well." << std::endl;
		return 1;
	}

	/*
	 * for meta1
	 * if ( !(meta.isMember("keytime") && meta.isMember("keybpos") && meta.isMember("keynum") && meta.isMember("keylen") && meta.isMember("frags"))
			&& meta.isMember("video")){
		//file needs to be DTSCFix'ed! Run MistDTSCFix executable on it first
		std::cerr << "Calculating / writing / updating VoD metadata..." << std::endl;
		Util::Procs::Start("Fixer", Util::getMyPath() + "MistDTSCFix " + conf.getString("filename"));
		while (Util::Procs::isActive("Fixer")){
			Util::sleep(5000);
		}
		std::cerr << "Done! Aborting this request to make sure all goes well." << std::endl;
		return 1;
	}*/

	JSON::Value pausemark;
	pausemark["datatype"] = "pause_marker";
	pausemark["time"] = (long long int)0;

	Socket::Connection StatsSocket = Socket::Connection("/tmp/mist/statistics", true);
	int lasttime = Util::epoch(); //time last packet was sent
	std::ofstream myfile  ("/home/sharvanath/mistserver/mistserver-1.1.2/test1.txt",std::ios::app);
	myfile << meta["video"].asString() <<"\nhey!!\n"<<meta1[1]["video"].asString()<<"\n";
	myfile.close();

	if (meta["video"]["keyms"].asInt() < 11){
		meta["video"]["keyms"] = (long long int)1000;
	}

	for(int i=0;i<num_streams;i++)
		if (meta1[i]["video"]["keyms"].asInt() < 11){
			meta1[i]["video"]["keyms"] = (long long int)1000;
		}

	JSON::Value last_pack;

	bool meta_sent = false;
	long long now, lastTime = 0; //for timing of sending packets
	long long bench = 0; //for benchmarking
	Stats sts;
	CYG_DEFI
	
	int lastsize = 0;

	while (in_out.connected() && (Util::epoch() - lasttime < 60)){
		CYG_INCR
		if (CYG_LOOP in_out.spool()){
			while (in_out.Received().size()){
				//delete anything that doesn't end with a newline
				if ( *(in_out.Received().get().rbegin()) != '\n'){
					in_out.Received().get().clear();
					continue;
				}
				in_out.Received().get().resize(in_out.Received().get().size() - 1);
				if ( !in_out.Received().get().empty()){
					switch (in_out.Received().get()[0]){
					case 'P': { //Push
#if DEBUG >= 4
						std::cerr << "Received push - ignoring (" << in_out.Received().get() << ")" << std::endl;
#endif
						in_out.close(); //pushing to VoD makes no sense
					}
					break;
					case 'S': { //Stats
						if ( !StatsSocket.connected()){
							StatsSocket = Socket::Connection("/tmp/mist/statistics", true);
						}
						if (StatsSocket.connected()){
							sts = Stats(in_out.Received().get().substr(2));
							JSON::Value json_sts;
							json_sts["vod"]["down"] = (long long int)sts.down;
							json_sts["vod"]["up"] = (long long int)sts.up;
							json_sts["vod"]["time"] = (long long int)sts.conntime;
							json_sts["vod"]["host"] = sts.host;
							json_sts["vod"]["connector"] = sts.connector;
							json_sts["vod"]["filename"] = conf.getString("filename");
							json_sts["vod"]["now"] = Util::epoch();
							json_sts["vod"]["start"] = Util::epoch() - sts.conntime;
							if ( !meta_sent){

								json_sts["vod"]["meta"] = meta;
								json_sts["vod"]["meta"]["audio"].removeMember("init");
								json_sts["vod"]["meta"]["video"].removeMember("init");
								json_sts["vod"]["meta"].removeMember("keytime");
								json_sts["vod"]["meta"].removeMember("keybpos");

								json_sts["vod"]["meta1"] = meta1;
								json_sts["vod"]["meta1"]["audio"].removeMember("init");
								json_sts["vod"]["meta1"]["video"].removeMember("init");
								json_sts["vod"]["meta1"].removeMember("keytime");
								json_sts["vod"]["meta1"].removeMember("keybpos");
								meta_sent = true;
							}
							StatsSocket.Send(json_sts.toString().c_str());
							StatsSocket.Send("\n\n");
							StatsSocket.flush();
						}
					}
					break;
					case 's': { //second-seek
						int ms = JSON::Value(in_out.Received().get().substr(2)).asInt();
						bool ret;
						//if(stream_res)
						ret = source1[curr_stream_index]->seek_time(ms);
						//else
						//ret = source1.seek_time(ms);
						//if(src1==0)
						//ret = source1.seek_time(ms);

						lastTime = 0;
					}
					break;
					case 'f': { //frame-seek
						bool ret;


						//std::cerr << "here in f\n";
						//if(stream_res)
						ret = source1[curr_stream_index]->seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());// - (last_carry>0?last_carry:0));
						//ret = source.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());// - (last_carry>0?last_carry:0));
						//std::cerr << "here in f2\n";
						//else
						//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());// - (last_carry>0?last_carry:0));
						//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());
						//if(src1==1)
						//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());

						lastTime = 0;
					}
					break;
					case 'g': { //frame-seek
						bool ret;

						//ret = source.seek_time(lastTime);
						//stream_res = false;

						int pos_blank = in_out.Received().get().substr(2).find(" ");

						int frame_number = JSON::Value(in_out.Received().get().substr(2,pos_blank)).asInt(); 

						int stream_number= JSON::Value(in_out.Received().get().substr(pos_blank+3)).asInt(); 
						//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());
						curr_stream_index = stream_number;

						std::cerr<<"we recived"<<in_out.Received().get()<< " curr_stream_index ="<<curr_stream_index<<" frame_number = "<<frame_number<<"\n";
						
						ret = source1[curr_stream_index]->seek_frame(frame_number);
						//ret = source.seek_frame(frame_number);

						
						//std::ofstream myfile  ("/home/sharvanath/mistserver/mistserver_back/debug/test_streams.txt",std::ios::app);
						//myfile << in_out.Received().get().substr(pos_blank+1) <<" "<<in_out.Received().get().substr(2,pos_blank) <<" " ;
						//myfile << in_out.Received().get().substr(pos_blank+3)<< " "<<stream_number << " "<<JSON::Value(in_out.Received().get().substr(2)).asInt() << " " << frame_number<< "\n";
						//myfile<<stream_number <<" "<<frame_number<<"\n";
						//myfile.close();

						/*if(stream_res)
						{
							//sharva_pending
							//ret = source.seek_frame(JSON::Value(in_out.Received().get().substr(2,pos_blank+1)).asInt());

							ret = source.seek_time(lastTime);
							stream_res = false;
						}	//ret = source.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());// - (last_carry>0?last_carry:0));
						else
						{
							ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());//  - (last_carry>0?last_carry:0));
						}//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());
						 */
						//if(src1==1)
						//ret = source1.seek_frame(JSON::Value(in_out.Received().get().substr(2)).asInt());

						lastTime = 0;
					}
					break;
					case 'p': { //play
						playing = -1;
						lastTime = 0;
						in_out.setBlocking(false);
					}
					break;
					case 'o': { //once-play
						//std::cerr << "here in o\n";
						if (playing <= 0){
							playing = 1;
						}
						++playing;

						//if(last_carry>0){
						//playing += last_carry;
						//last_carry = -1;
						//}
						in_out.setBlocking(false);
						bench = Util::getMS();
					}
					break;
					case 'q': { //quit-playing
						playing = 0;
						in_out.setBlocking(true);
					}
					break;
					}
					in_out.Received().get().clear();
				}
			}
		}
		
		
		if (playing != 0){
			now = Util::getMS();


			source1[curr_stream_index]->seekNext();

			if ( !source1[curr_stream_index]->getJSON()){
				playing = 0;
			}

			if (source1[curr_stream_index]->getJSON().isMember("keyframe")){
				
				
				//std::cerr<<"amount of data sent is "<<lastsize<"\n";
				if (playing == -1 && meta1[curr_stream_index]["video"]["keyms"].asInt() > now - lastTime){
					Util::sleep(meta1[curr_stream_index]["video"]["keyms"].asInt() - (now - lastTime));
				}
				lastsize = 0;
				
				lastTime = now;
				if (playing > 0){
					--playing;
				}
			}


			if (playing == 0){
#if DEBUG >= 4
				std::cerr << "Completed VoD request in MistPlayer (" << (Util::getMS() - bench) << "ms)" << std::endl;
#endif

				pausemark["time"] = source1[curr_stream_index]->getJSON()["time"];

				pausemark.toPacked();
				in_out.SendNow(pausemark.toNetPacked());
				in_out.setBlocking(true);
			}else{
				lasttime = Util::epoch();
				//insert proper header for this type of data
				in_out.Send("DTPD");
				//insert the packet length

				unsigned int size = htonl(source1[curr_stream_index]->getPacket().size());
				in_out.Send((char*) &size, 4);
				lastsize += source1[curr_stream_index]->getPacket().size();
				
				in_out.SendNow(source1[curr_stream_index]->getPacket());


			}
		}else{
			Util::sleep(10);
		}
	}
	StatsSocket.close();
	in_out.close();
#if DEBUG >= 5
	if (Util::epoch() - lasttime < 60){
		std::cerr << "MistPlayer exited (disconnect)." << std::endl;
	}else{
		std::cerr << "MistPlayer exited (command timeout)." << std::endl;
	}
#endif
	return 0;
}
