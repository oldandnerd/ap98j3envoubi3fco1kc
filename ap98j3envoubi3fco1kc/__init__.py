import aiohttp
import asyncio
import hashlib
import logging
import random
import re
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List
from wordsegment import load, segment
from exorde_data import Item, Content, Title, Author, CreatedAt, Url, Domain
from aiohttp import ClientConnectorError

logging.basicConfig(level=logging.INFO)
MANAGER_IP = "http://192.227.159.3:8000"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
MAX_CONCURRENT_TASKS = 30
MAX_RETRIES_PROXY = 5  # Maximum number of retries for 503 errors

load()  # Load the wordsegment library data

# Initialize subreddit lists
# Initialize subreddit lists
"""subreddits_top_225 = [
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/all",
    "r/AITAH",
    "r/AITAH",
    "r/AITAH",
    "r/AmItheAsshole",
    "r/AmItheAsshole",
    "r/AlgorandOfficial",
    "r/almosthomeless",
    "r/altcoin",
    "r/amcstock",
    "r/Anarcho_Capitalism",
    "r/announcements",
    "r/announcements",
    "r/announcements",
    "r/announcements",
    "r/antiwork",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/AskReddit",
    "r/asktrading",
    "r/Banking",
    "r/baseball",
    "r/binance",
    "r/Bitcoin",
    "r/Bitcoin",
    "r/Bitcoin",
    "r/bitcoin",
    "r/BitcoinBeginners",
    "r/Bitcoincash",
    "r/BitcoinMarkets",
    "r/books",
    "r/btc",
    "r/btc",
    "r/btc",
    "r/budget",
    "r/BullTrader",
    "r/Buttcoin",
    "r/cardano",
    "r/China",
    "r/CoinBase",
    "r/CreditCards",
    "r/Crypto",
    "r/Crypto_General",
    "r/Cryptocurrencies",
    "r/Cryptocurrencies",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrency",
    "r/CryptoCurrencyClassic",
    "r/CryptocurrencyMemes",
    "r/CryptoCurrencyTrading",
    "r/CryptoMarkets",
    "r/CryptoMoonShots",
    "r/CryptoMoonShots",
    "r/CryptoMarkets",
    "r/CryptoTechnology",
    "r/Damnthatsinteresting",
    "r/dataisbeautiful",
    "r/defi",
    "r/defi",
    "r/Dividends",
    "r/dogecoin",
    "r/dogecoin",
    "r/dogecoin",
    "r/dogecoin",
    "r/Economics",
    "r/Economics",
    "r/Economics",
    "r/eth",
    "r/ethereum",
    "r/ethereum",
    "r/ethereum",
    "r/ethereum",
    "r/ethermining",
    "r/ethfinance",
    "r/ethstaker",
    "r/ethtrader",
    "r/ethtrader",
    "r/ethtrader",
    "r/etoro",
    "r/etoro",
    "r/Europe",
    "r/facepalm",
    "r/facepalm",
    "r/facepalm",
    "r/facepalm",
    "r/fatFIRE",
    "r/Finance",
    "r/Finance",
    "r/Finance",
    "r/FinanceNews",
    "r/FinanceNews",
    "r/FinanceNews",
    "r/FinanceStudents",
    "r/FinancialCareers",
    "r/financialindependence",
    "r/FinancialPlanning",
    "r/financialplanning",
    "r/forex",
    "r/formula1",
    "r/france",
    "r/Frugal",
    "r/Futurology",
    "r/gaming",
    "r/Germany",
    "r/GME",
    "r/ico",
    "r/investing",
    "r/investor",
    "r/jobs",
    "r/leanfire",
    "r/ledgerwallet",
    "r/litecoin",
    "r/MiddleClassFinance",
    "r/Monero",
    "r/Monero",
    "r/nanocurrency",
    "r/NFT",
    "r/NoStupidQuestions",
    "r/NoStupidQuestions",
    "r/NoStupidQuestions",
    "r/NoStupidQuestions",
    "r/passive_income",
    "r/pennystocks",
    "r/personalfinance",
    "r/PFtools",
    "r/politics",
    "r/politics",
    "r/politics",
    "r/politics",
    "r/politics",
    "r/politics",
    "r/povertyfinance",
    "r/povertyfinance",
    "r/povertyfinance",
    "r/realestateinvesting",
    "r/retirement",
    "r/Ripple",
    "r/robinhood",
    "r/robinhood",
    "r/Showerthoughts",
    "r/soccer",
    "r/space",
    "r/sports",
    "r/sports",
    "r/sports",
    "r/Stellar",
    "r/stockmarket",
    "r/stockmarket",
    "r/Stocks",
    "r/Stocks",
    "r/Stocks",
    "r/StudentLoans",
    "r/tax",
    "r/technicalraptor",
    "r/technology",
    "r/technology",
    "r/technology",
    "r/Tether",
    "r/todayilearned",
    "r/todayilearned",
    "r/todayilearned",
    "r/todayilearned",
    "r/trading",
    "r/trading",
    "r/trading",
    "r/tradingreligion",
    "r/unitedkingdom",
    "r/unpopularopinion",
    "r/ValueInvesting",
    "r/ValueInvesting",
    "r/ValueInvesting",
    "r/Wallstreet",
    "r/WallStreetBets",
    "r/WallStreetBets",
    "r/WallStreetBets",
    "r/WallStreetBetsCrypto",
    "r/Wallstreetsilver",
    "r/WhitePeopleTwitter",
    "r/WhitePeopleTwitter",
    "r/WhitePeopleTwitter",
    "r/WhitePeopleTwitter",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
    "r/worldnews",
    ###
    "r/BaldursGate3",
    "r/teenagers",
    "r/BaldursGate3",
    "r/teenagers",
    "r/BaldursGate3",
    "r/teenagers",
    "r/BaldursGate3",
    "r/teenagers",
    "r/BigBrother",
    "r/BigBrother",
    "r/BigBrother",
    "r/wallstreetbets",
    "r/wallstreetbets",
    "r/namenerds",
    "r/Eldenring",
    "r/Unexpected",
    "r/NonCredibleDefense",
    "r/wallstreetbets",
    "r/news",
    "r/news",
    "r/news",
    "r/mildlyinteresting",  
    "r/RandomThoughts",
    "r/ireland",
    "r/france",
    "r/ireland",
    "r/de",
    "r/ireland",
    "r/unitedkingdom", "r/AskUK", "r/CasualUK", "r/britishproblems",
    "r/canada", "r/AskCanada", "r/onguardforthee", "r/CanadaPolitics",
    "r/australia", "r/AskAnAustralian", "r/straya", "r/sydney",
    "r/india", "r/AskIndia", "r/bollywood", "r/Cricket", "r/Slovenia", "r/indiadiscussion",
    "r/germany", "r/de", "r/LearnGerman", "r/germusic",
    "r/france", "r/French", "r/paris", "r/europe", "r/relacionamentos",
    "r/japan", "r/japanlife", "r/newsokur", "r/learnjapanese",
    "r/brasil", "r/brasilivre", "r/riodejaneiro", "r/saopaulo",
    "r/mexico", "r/MexicoCity", "r/spanish", "r/yo_espanol",
    # 50 Most Popular News, Politics, and Finance/Economics Subreddits
    "r/news", "r/worldnews", "r/UpliftingNews", "r/nottheonion", "r/TrueReddit",
    "r/politics", "r/PoliticalDiscussion", "r/worldpolitics", "r/neutralpolitics", "r/Ask_Politics",
    "r/personalfinance", "r/investing", "r/StockMarket", "r/financialindependence", "r/economics",
    "r/TaylorSwift","r/TaylorSwift"
    # 50 Simply Relevant/Popular Subreddits
    "r/AskReddit", "r/IAmA", "r/funny", "r/pics", "r/gaming", "r/aww", "r/todayilearned",
    "r/science", "r/technology", "r/worldnews", "r/Showerthoughts", "r/books", "r/movies",
    "r/Music", "r/Art", "r/history", "r/EarthPorn", "r/food", "r/travel", "r/fitness", "r/DIY",
    "r/LifeProTips", "r/explainlikeimfive", "r/dataisbeautiful", "r/futurology", "r/WritingPrompts",
    "r/nosleep", "r/personalfinance", "r/photography", "r/NatureIsFuckingLit", "r/Advice",
    "r/askscience", "r/gadgets", "r/funny", "r/pics", "r/gaming", "r/aww", "r/todayilearned",
    "r/science", "r/technology", "r/worldnews", "r/Showerthoughts", "r/books", "r/movies",
    "r/Music", "r/Art", "r/history", "r/EarthPorn", "r/food", "r/travel", "r/fitness", "r/DIY",
    "r/LifeProTips", "r/explainlikeimfive", "r/dataisbeautiful", "r/futurology", "r/WritingPrompts"
]


subreddits_top_1000 = [
    "r/AskReddit","r/AmItheAsshole","r/teenagers","r/NoStupidQuestions","r/BaldursGate3","r/facepalm","r/AITAH","r/TaylorSwift","r/soccer","r/WhitePeopleTwitter",
    "r/CryptoCurrency","r/FreeKarma4All","r/mildlyinfuriating","r/relationship_advice","r/politics","r/wallstreetbets","r/movies","r/gaming","r/UFOs","r/unpopularopinion",
    "r/PublicFreakout","r/antiwork","r/diablo4","r/amiugly","r/CFB","r/LiverpoolFC","r/nba","r/Damnthatsinteresting","r/AskUK","r/ask",
    "r/MonopolyGoTrading","r/nfl","r/therewasanattempt","r/SquaredCircle","r/worldnews","r/AskMen","r/memes","r/amiwrong","r/pcmasterrace","r/cats",
    "r/GachaClub","r/TrueOffMyChest","r/OnePiece","r/pathofexile","r/TikTokCringe","r/neoliberal","r/todayilearned","r/biggboss","r/Overwatch","r/coys",
    "r/TEMUcodeShare","r/news","r/KGBTR","r/FortNiteBR","r/golf","r/Genshin_Impact","r/deadbydaylight","r/leagueoflegends","r/Eldenring","r/baseball",
    "r/meirl","r/MadeMeSmile","r/ImTheMainCharacter","r/de","r/Philippines","r/Minecraft","r/Unexpected","r/HolUp","r/TwoXChromosomes","r/FantasyPL",
    "r/Serverlife","r/canada","r/MortalKombat","r/CrazyFuckingVideos","r/videogames","r/Christianity","r/NonCredibleDefense","r/redscarepod","r/shitposting","r/Karma4Free",
    "r/Market76","r/europe","r/DnD","r/remnantgame","r/pics","r/EscapefromTarkov","r/atheism","r/anime","r/futebol","r/namenerds",
    "r/conspiracy","r/weddingdress","r/explainlikeimfive","r/unitedkingdom","r/chelseafc","r/RoastMe","r/StupidFood","r/apexlegends","r/alevel","r/TooAfraidToAsk",
    "r/DestinyTheGame","r/BlackPeopleTwitter","r/Weird","r/Warthunder","r/shittytattoos","r/FaceRatings","r/TrueUnpopularOpinion","r/argentina","r/pokemon","r/OldSchoolCool",
    "r/television","r/Presidents","r/ufc","r/Starfield","r/AntiTrumpAlliance","r/CasualUK","r/JEENEETards","r/mildlyinteresting","r/PurplePillDebate","r/Canada_sub",
    "r/OnePiecePowerScaling","r/australia","r/2007scape","r/technology","r/tifu","r/newzealand","r/nrl","r/Destiny","r/Warframe","r/PoliticalCompassMemes",
    "r/PeterExplainsTheJoke","r/horror","r/Gunners","r/changemyview","r/Spiderman","r/barstoolsports","r/StreetFighter","r/totalwar","r/196","r/Teachers",
    "r/BestofRedditorUpdates","r/doordash","r/Parenting","r/ZeducationSubmissions","r/funny","r/dating_advice","r/BeAmazed","r/ireland","r/sex","r/Italia",
    "r/PokemonScarletViolet","r/AnarchyChess","r/motorcycles","r/AusFinance","r/reddevils","r/ChatGPT","r/Torontobluejays","r/Tinder","r/OUTFITS","r/SteamDeck",
    "r/h3h3productions","r/playstation","r/Brawlstars","r/whatisthisbug","r/sweden","r/gardening","r/WWE","r/harrypotter","r/MapPorn","r/LosAngeles",
    "r/dating","r/autism","r/Naruto","r/FunnyandSad","r/UkraineWarVideoReport","r/buildapc","r/Animemes","r/grandorder","r/indonesia","r/pcgaming",
    "r/Advice","r/ffxiv","r/marvelstudios","r/Youmo","r/relationships","r/FragReddit","r/Tekken","r/serbia","r/DunderMifflin","r/Mariners",
    "r/personalfinance","r/Romania","r/masterduel","r/polls","r/ThatsInsane","r/PremierLeague","r/startrek","r/Marriage","r/DeathBattleMatchups","r/EDH",
    "r/DotA2","r/RandomThoughts","r/BollyBlindsNGossip","r/LoveIslandUSA","r/whatcarshouldIbuy","r/Fauxmoi","r/fivenightsatfreddys","r/BravoRealHousewives","r/transformers","r/AskAnAmerican",
    "r/Genshin_Impact_Leaks","r/boxoffice","r/brasil","r/PersonalFinanceCanada","r/offmychest","r/NYYankees","r/BatmanArkham","r/DesignMyRoom","r/tennis","r/chile",
    "r/bleach","r/exmormon","r/travel","r/AmericaBad","r/tjournal_refugees","r/malelivingspace","r/orioles","r/tearsofthekingdom","r/aliens","r/mexico",
    "r/LivestreamFail","r/phillies","r/AskOldPeople","r/uknews","r/nursing","r/askgaybros","r/pettyrevenge","r/melbourne","r/trees","r/TwoBestFriendsPlay",
    "r/WTF","r/PokemonHome","r/Showerthoughts","r/MovieSuggestions","r/entertainment","r/AskMiddleEast","r/fut","r/StarWars","r/boston","r/MMA",
    "r/formula1","r/fantasyfootball","r/books","r/nextfuckinglevel","r/Doppleganger","r/hockey","r/LifeProTips","r/HomeImprovement","r/AussieTikTokSnark","r/batman",
    "r/Turkey","r/ukpolitics","r/Denmark","r/wow","r/MechanicAdvice","r/firstimpression","r/ValorantCompetitive","r/wholesomememes","r/Pikmin","r/conservativeterrorism",
    "r/army","r/careerguidance","r/delhi","r/Cooking","r/OriginalCharacter","r/NotHowGirlsWork","r/ADHD","r/vancouver","r/XboxSeriesX","r/confessions",
    "r/trans","r/FreeKarma4You","r/DarkAndDarker","r/adhdwomen","r/Grimdank","r/GenX","r/AMA","r/40kLore","r/IAmTheMainCharacter","r/geometrydash",
    "r/AEWOfficial","r/PokemonGoFriends","r/tipofmytongue","r/legaladvice","r/tf2","r/reddeadredemption","r/france","r/dankmemes","r/StardewValley","r/florida",
    "r/stopdrinking","r/gtaonline","r/NoFap","r/Braves","r/femalehairadvice","r/bjj","r/india","r/Costco","r/Suomi","r/PcBuild",
    "r/daddit","r/CasualPT","r/lookismcomic","r/Ningen","r/italy","r/adultingph","r/JoeRogan","r/whenthe","r/jobs","r/PS5",
    "r/fcbayern","r/IndianTeenagers","r/MemePiece","r/fightporn","r/DeepRockGalactic","r/BocaJuniors","r/selfie","r/Letterboxd","r/nhl","r/malaysia",
    "r/BabyBumps","r/DMZ","r/hungary","r/Conservative","r/skyrim","r/lego","r/football","r/vegan","r/90DayFiance","r/czech",
    "r/Polska","r/SFGiants","r/childfree","r/Piracy","r/Finanzen","r/DynastyFF","r/Truckers","r/tattooadvice","r/Hololive","r/PokemonTCG",
    "r/dndnext","r/thefighterandthekid","r/GunAccessoriesForSale","r/LeopardsAteMyFace","r/BigBrother","r/portugal","r/PoliticalHumor","r/Games","r/fo76","r/btd6",
    "r/exmuslim","r/terriblefacebookmemes","r/Watches","r/mlb","r/CombatFootage","r/starterpacks","r/AskArgentina","r/Patriots","r/shrooms","r/oddlyterrifying",
    "r/SubSimGPT2Interactive","r/Padres","r/FinalFantasy","r/ClashRoyale","r/runescape","r/Cricket","r/chicago","r/Dragonballsuper","r/rugbyunion","r/rolex",
    "r/Seattle","r/IndiaSpeaks","r/halo","r/oddlysatisfying","r/RealEstate","r/pokemongo","r/Rainbow6","r/travisscott","r/bloxfruits","r/Wrasslin",
    "r/magicTCG","r/Catholicism","r/Accounting","r/Sims4","r/thesopranos","r/Fantasy","r/HouseOfTheDragon","r/CATHELP","r/PokemonUnite","r/ontario",
    "r/HistoryMemes","r/maybemaybemaybe","r/Music","r/Austria","r/traaaaaaannnnnnnnnns2","r/Adulting","r/astrologymemes","r/saudiarabia","r/Terraria","r/Wellthatsucks",
    "r/nederlands","r/Tattoocoverups","r/Construction","r/unitedstatesofindia","r/work","r/UkrainianConflict","r/destiny2","r/KingOfTheHill","r/notinteresting","r/Chiraqology",
    "r/realmadrid","r/cscareerquestions","r/BattleBitRemastered","r/CuratedTumblr","r/ExplainTheJoke","r/desijo_b","r/weed","r/Justrolledintotheshop","r/VaushV","r/IdiotsInCars",
    "r/sanfrancisco","r/bayarea","r/AskAnAustralian","r/classicwow","r/askSingapore","r/NYStateOfMind","r/singularity","r/NASCAR","r/lotrmemes","r/AskACanadian",
    "r/KafkaMains","r/AirForce","r/Marvel","r/Austin","r/airsoft","r/ConeHeads","r/counting","r/rusAskReddit","r/greece","r/Piratefolk",
    "r/sysadmin","r/Breath_of_the_Wild","r/trashy","r/AskWomenOver30","r/mtg","r/Mommit","r/jerkofftoceleb","r/hearthstone","r/AskOuija","r/Frugal",
    "r/shittyfoodporn","r/CasualConversation","r/homeowners","r/cars","r/Ohio","r/rupaulsdragrace","r/awfuleverything","r/BrandNewSentence","r/RocketLeagueEsports","r/furry_irl",
    "r/femboy","r/lostarkgame","r/Andjustlikethat","r/baseballcards","r/80s","r/exjw","r/Astros","r/electricvehicles","r/WouldYouRather","r/KimetsuNoYaiba",
    "r/nbacirclejerk","r/popheads","r/TheDeprogram","r/loseit","r/lgbt","r/YuB","r/REBubble","r/toronto","r/memesopdidnotlike","r/RimWorld",
    "r/perth","r/OnePunchMan","r/comics","r/EnoughMuskSpam","r/pregnant","r/Warhammer40k","r/FFXVI","r/NameMyCat","r/moreplatesmoredates","r/ftm",
    "r/CleaningTips","r/TrueChristian","r/Dodgers","r/MobileLegendsGame","r/IndianGaming","r/chess","r/asoiaf","r/JRPG","r/IASIP","r/raisedbynarcissists",
    "r/PhotoshopRequest","r/AskNYC","r/seinfeld","r/AbruptChaos","r/Mortalkombatleaks","r/masseffect","r/Anticonsumption","r/SpidermanPS4","r/askhungary","r/angelsbaseball",
    "r/discordVideos","r/AskConservatives","r/london","r/mbti","r/inthenews","r/walmart","r/EASportsFC","r/talk_hunfluencers","r/Firearms","r/HuntShowdown",
    "r/GenZ","r/povertyfinance","r/UKPersonalFinance","r/kpopthoughts","r/MtF","r/KitchenConfidential","r/InstacartShoppers","r/Xennials","r/norge","r/minnesota",
    "r/AFL","r/Diablo","r/DIY","r/CarsIndia","r/actuallesbians","r/greentext","r/Undertale","r/maui","r/UnethicalLifeProTips","r/croatia",
    "r/VALORANT","r/mapporncirclejerk","r/BMW","r/TheOwlHouse","r/Ben10","r/delta","r/beauty","r/hiphopheads","r/LeagueOfMemes","r/RocketLeague",
    "r/yeezys","r/roblox","r/LateStageCapitalism","r/youngpeopleyoutube","r/MandJTV","r/ShuumatsuNoValkyrie","r/playboicarti","r/ukraine","r/recruitinghell","r/germany",
    "r/StableDiffusion","r/CallOfDutyMobile","r/kollywood","r/Gamingcirclejerk","r/Scotland","r/goodanimemes","r/Millennials","r/SmashBrosUltimate","r/Monopoly_GO","r/Boxing",
    "r/3Dprinting","r/boardgames","r/BITSPilani","r/Plumbing","r/tacticalgear","r/Barca","r/DemonSlayerAnime","r/discgolf","r/danganronpa","r/2american4you",
    "r/AskFrance","r/Bumble","r/electricians","r/PrequelMemes","r/FanFiction","r/thebachelor","r/MyHeroAcadamia","r/Boruto","r/90dayfianceuncensored","r/developersIndia",
    "r/handbags","r/AmITheDevil","r/cycling","r/Fallout","r/amcstock","r/ottawa","r/Quebec","r/Guildwars2","r/yugioh","r/UKJobs",
    "r/VeteransBenefits","r/Chainsawfolk","r/projectzomboid","r/whatisit","r/BrandonDE","r/Fishing","r/GregDoucette","r/truerateme","r/stocks","r/China_irl",
    "r/fromsoftware","r/texas","r/OffMyChestPH","r/formuladank","r/ClassicRock","r/ProgrammerHumor","r/AutismInWomen","r/starcitizen","r/singapore","r/Denver",
    "r/stunfisk","r/desabafos","r/ar15","r/BeelcitosMemes","r/dbz","r/Weddingattireapproval","r/Tools","r/lastimages","r/belowdeck","r/IndianDankMemes",
    "r/VerifiedFeet","r/MarvelStudiosSpoilers","r/ClashOfClans","r/meme","r/PhoenixSC","r/CasualPH","r/malehairadvice","r/TwoSentenceHorror","r/xqcow","r/NoRules",
    "r/PokemonSleep","r/Gundam","r/Residency","r/distressingmemes","r/BlueArchive","r/brasilivre","r/islam","r/asktransgender","r/Pathfinder2e","r/moviecritic",
    "r/SkincareAddiction","r/LoveIslandTV","r/UberEATS","r/WarhammerCompetitive","r/manga","r/antitrampo","r/Aquariums","r/Kenya","r/lanadelrey","r/fountainpens",
    "r/mariokart","r/nope","r/HVAC","r/chessbeginners","r/real_China_irl","r/Sephora","r/BG3Builds","r/gameofthrones","r/residentevil","r/Netherlands",
    "r/StudentLoans","r/AskALiberal","r/clevercomebacks","r/DokkanBattleCommunity","r/japanlife","r/AmazonFC","r/crochet","r/Kappachino","r/Sneakers","r/Hawaii",
    "r/sports","r/InstaCelebsGossip","r/Pandabuy","r/Eminem","r/farialimabets","r/MagicArena","r/pittsburgh","r/blankies","r/TheSilphRoad","r/nova",
    "r/lawncare","r/TeenMomOGandTeenMom2","r/EngagementRings","r/debbiethepetladysnark","r/iphone","r/HilariaBaldwin","r/poker","r/flying","r/auckland","r/Scams",
    "r/JoeyBdezSnark2","r/sportsbook","r/TroChuyenLinhTinh","r/technicallythetruth","r/evilautism","r/SeattleWA","r/Colombia","r/AnimalCrossing","r/dataisbeautiful","r/Bitcoin",
    "r/GlobalOffensive","r/Deltarune","r/footballmanagergames","r/whowouldwin","r/punk","r/starwarsmemes","r/NewParents","r/thenetherlands","r/Louisville","r/aww",
    "r/canadahousing","r/overwatch2","r/AustralianPolitics","r/ContagiousLaughter","r/KidsAreFuckingStupid","r/FifaCareers","r/TeslaModel3","r/lonely","r/MeJulgue","r/CrusaderKings",
    "r/Kanye","r/OutsideLands","r/bald","r/LegalAdviceUK","r/DragonballLegends","r/ScottishFootball","r/USPS","r/whatsthisplant","r/USMC","r/crossdressing",
    "r/howardstern","r/CitiesSkylines","r/coolguides","r/InfluencergossipDK","r/StellarCannaCoin","r/RoastMyCar","r/zelda","r/neopets","r/Berserk","r/DiWHY",
    "r/NoMansSkyTheGame","r/BBBY","r/TheSimpsons","r/FUTMobile","r/NBA2k","r/ChoosingBeggars","r/wichsbros_DEU2023","r/AmITheAngel","r/whatsthisbug","r/fantanoforever",
    "r/houston","r/Guitar","r/Mario","r/datingoverforty","r/WorkReform","r/FashionReps","r/SWGalaxyOfHeroes","r/pokemontrades","r/femboymemes","r/DC_Cinematic",
    "r/HighStrangeness","r/RedditPregunta","r/Bolehland","r/beards","r/Random_Acts_Of_Amazon","r/LSD","r/worldbuilding","r/dccomicscirclejerk","r/MySingingMonsters","r/Persona5",
    "r/geography","r/woodworking","r/southpark","r/NewTubers","r/BruceDropEmOff","r/videos","r/Whatcouldgowrong","r/realhousewives","r/TrueCrimeDiscussion","r/skyrimmods",
    "r/redditmoment","r/Columbus","r/nvidia","r/washingtondc","r/Competitiveoverwatch","r/KerbalSpaceProgram","r/Transformemes","r/houseplants","r/collapse","r/CroatiaAlt",
    "r/blackdesertonline","r/antinatalism","r/Teenager_Polls","r/northernireland","r/martialarts","r/JustUnsubbed","r/saltierthankrayt","r/ChainsawMan","r/Economics","r/PathOfExileBuilds",
    "r/steak","r/KUWTKsnark","r/eu4","r/Broadway","r/HypixelSkyblock","r/denvernuggets","r/fantasybaseball","r/CPTSD","r/NFA","r/comicbookmovies",
    "r/ksi","r/Norway","r/PetiteFashionAdvice","r/nottheonion","r/Guiltygear","r/science","r/interestingasfuck","r/virtualreality","r/rareinsults","r/bangalore",
    "r/TIKTOKSNARKANDGOSSIP","r/yeat_","r/whatisthiscar","r/DCcomics","r/nyc","r/ShittyMapPorn","r/Fighters","r/insanepeoplefacebook","r/HousingUK","r/BattleForDreamIsland",
    "r/forhonor","r/Stellaris","r/MakeupAddiction","r/phish","r/askTO","r/MaliciousCompliance","r/battlecats","r/gamingsuggestions","r/BisexualTeens","r/NonPoliticalTwitter",
    "r/AppleWatch","r/Qult_Headquarters","r/armoredcore","r/thewalkingdead","r/SCJerk","r/HotWheels","r/yakuzagames","r/xmen","r/CatAdvice","r/CUETards",
    "r/Winnipeg","r/StardustCrusaders","r/trippieredd","r/space","r/halifax","r/TattooDesigns","r/EntitledPeople","r/WatchPeopleDieInside","r/conspiracy_commons","r/ToiletPaperUSA",
    "r/UPSers","r/ARK","r/ThePPShow","r/Sub4Sub","r/SonicTheHedgehog","r/RepTime","r/EuSouOBabaca","r/IndianBoysOnTinder","r/CasualRO","r/bindingofisaac",
    "r/4chan","r/GunMemes","r/coins","r/tressless","r/csgo","r/Audi","r/Nanny","r/tacobell","r/ironscape","r/orangecounty",
    "r/TerrifyingAsFuck","r/Edmonton","r/ManchesterUnited","r/Ultrakill","r/Amberverse_","r/MensRights","r/AmazonDSPDrivers","r/preppers","r/dubai","r/valheim",
    "r/MinecraftMemes","r/bigdickproblems","r/Funnymemes","r/Pikabu","r/okbuddychicanery","r/egg_irl","r/Ticos","r/FirstTimeHomeBuyer","r/NatureIsFuckingLit","r/SelfieOver25",
    "r/wildrift","r/Smite","r/30PlusSkinCare","r/Abortiondebate","r/AzureLane","r/Memes_Of_The_Dank","r/PokemonRoleplays","r/xbox","r/IThinkYouShouldLeave","r/WaltDisneyWorld",
    "r/UrbanHell","r/liseliler","r/Miata","r/CarTalkUK","r/selfimprovement","r/GooglePixel","r/RaidShadowLegends","r/Genshin_Memepact","r/RomanceBooks","r/lyftdrivers",
    "r/Watchexchange","r/vexillology","r/BreakUps","r/newjersey","r/FreeCompliments","r/brisbane","r/PoliticalCompass","r/Amd","r/CODZombies","r/DevilMayCry",
    "r/AskMechanics","r/Vanderpumpaholics","r/AdorableNudes","r/NoJumper","r/Totaldrama","r/mylittlepony","r/ffxivdiscussion","r/bisexual","r/indiadiscussion","r/kingcobrajfs",
    "r/MTB","r/WFH","r/snappijuorut","r/Dallas","r/AO3","r/breakingbad","r/ufo","r/DebateReligion","r/PoliticalMemes","r/Gunpla",
    "r/Rabbits","r/Slovenia","r/ich_iel","r/splatoon","r/BanPitBulls","r/suggestmeabook","r/FLMedicalTrees","r/relacionamentos","r/FireEmblemHeroes","r/GoodAssSub",
    "r/HFY","r/19684","r/RobloxAvatars","r/whatisthisthing","r/OtomeIsekai","r/Kengan_Ashura","r/JUSTNOMIL","r/USCIS","r/homelab","r/gundeals",
    "r/doctorsUK","r/Entrepreneur","r/bluey","r/careeradvice","r/kolkata","r/arborists","r/TheMajorityReport","r/4Runner","r/GalaxyFold","r/gaybros",
    "r/Calgary","r/furry","r/csMajors","r/Bedbugs","r/DBZDokkanBattle","r/mumbai","r/popheadscirclejerk","r/marvelmemes","r/Egypt","r/Topster",
]

"""


subreddits_top_225 = [
    "r/announcements",
    "r/funny",
    "r/AskReddit",
    "r/gaming",
    "r/aww",
    "r/Music",
    "r/worldnews",
    "r/pics",
    "r/movies",
    "r/todayilearned",
    "r/science",
    "r/videos",
    "r/Showerthoughts",
    "r/news",
    "r/Jokes",
    "r/askscience",
    "r/food",
    "r/EarthPorn",
    "r/IAmA",
    "r/nottheonion",
    "r/DIY",
    "r/gifs",
    "r/books",
    "r/space",
    "r/Art",
    "r/explainlikeimfive",
    "r/LifeProTips",
    "r/memes",
    "r/mildlyinteresting",
    "r/sports",
    "r/gadgets",
    "r/Documentaries",
    "r/blog",
    "r/dataisbeautiful",
    "r/UpliftingNews",
    "r/GetMotivated",
    "r/tifu",
    "r/photoshopbattles",
    "r/listentothis",
    "r/history",
    "r/philosophy",
    "r/Futurology",
    "r/OldSchoolCool",
    "r/nosleep",
    "r/television",
    "r/personalfinance",
    "r/InternetIsBeautiful",
    "r/WritingPrompts",
    "r/creepy",
    "r/TwoXChromosomes",
    "r/technology",
    "r/wallstreetbets",
    "r/wholesomememes",
    "r/interestingasfuck",
    "r/Fitness",
    "r/AdviceAnimals",
    "r/lifehacks",
    "r/politics",
    "r/NatureIsFuckingLit",
    "r/Unexpected",
    "r/oddlysatisfying",
    "r/relationship_advice",
    "r/WTF",
    "r/travel",
    "r/Minecraft",
    "r/dadjokes",
    "r/nextfuckinglevel",
    "r/Whatcouldgowrong",
    "r/pcmasterrace",
    "r/facepalm",
    "r/MadeMeSmile",
    "r/me_irl",
    "r/Damnthatsinteresting",
    "r/buildapc",
    "r/leagueoflegends",
    "r/AnimalsBeingDerps",
    "r/AnimalsBeingBros",
    "r/dankmemes",
    "r/BlackPeopleTwitter",
    "r/Tinder",
    "r/PS4",
    "r/place",
    "r/CryptoCurrency",
    "r/bestof",
    "r/tattoos",
    "r/AnimalsBeingJerks",
    "r/nba",
    "r/HistoryMemes",
    "r/anime",
    "r/gardening",
    "r/photography",
    "r/mildlyinfuriating",
    "r/Awwducational",
    "r/WatchPeopleDieInside",
    "r/Parenting",
    "r/malefashionadvice",
    "r/EatCheapAndHealthy",
    "r/FoodPorn",
    "r/programming",
    "r/AmItheAsshole",
    "r/stocks",
    "r/BikiniBottomTwitter",
    "r/trippinthroughtime",
    "r/woodworking",
    "r/AmItheAsshole",
    "r/instant_regret",
    "r/PublicFreakout",
    "r/pokemon",
    "r/NintendoSwitch",
    "r/ContagiousLaughter",
    "r/rarepuppers",
    "r/dating_advice",
    "r/woahdude",
    "r/AskMen",
    "r/pokemongo",
    "r/gonewild",
    "r/BeAmazed",
    "r/reactiongifs",
    "r/Overwatch",
    "r/HighQualityGifs",
    "r/itookapicture",
    "r/IdiotsInCars",
    "r/Outdoors",
    "r/xboxone",
    "r/HumansBeingBros",
    "r/YouShouldKnow",
    "r/AskWomen",
    "r/iphone",
    "r/streetwear",
    "r/cats",
    "r/boardgames",
    "r/PewdiepieSubmissions",
    "r/nonononoyes",
    "r/apple",
    "r/Eyebleach",
    "r/loseit",
    "r/soccer",
    "r/nsfw",
    "r/drawing",
    "r/Cooking",
    "r/starterpacks",
    "r/cars",
    "r/MovieDetails",
    "r/NetflixBestOf",
    "r/GifRecipes",
    "r/europe",
    "r/confession",
    "r/recipes",
    "r/cursedcomments",
    "r/relationships",
    "r/blackmagicfuckery",
    "r/battlestations",
    "r/MakeupAddiction",
    "r/HistoryPorn",
    "r/learnprogramming",
    "r/backpacking",
    "r/RealGirls",
    "r/scifi",
    "r/therewasanattempt",
    "r/keto",
    "r/KidsAreFuckingStupid",
    "r/HolUp",
    "r/entertainment",
    "r/socialskills",
    "r/howto",
    "r/HomeImprovement",
    "r/Games",
    "r/slowcooking",
    "r/gameofthrones",
    "r/DeepIntoYouTube",
    "r/CrappyDesign",
    "r/Sneakers",
    "r/BetterEveryLoop",
    "r/raspberry_pi",
    "r/OutOfTheLoop",
    "r/humor",
    "r/biology",
    "r/youtubehaiku",
    "r/pcgaming",
    "r/Wellthatsucks",
    "r/foodhacks",
    "r/teenagers",
    "r/hardware",
    "r/camping",
    "r/NoStupidQuestions",
    "r/frugalmalefashion",
    "r/offmychest",
    "r/trashy",
    "r/assholedesign",
    "r/atheism",
    "r/MurderedByWords",
    "r/ChildrenFallingOver",
    "r/DnD",
    "r/marvelstudios",
    "r/mac",
    "r/nutrition",
    "r/nfl",
    "r/unpopularopinion",
    "r/Filmmakers",
    "r/bodyweightfitness"
]

def weighted_choice(subreddits_225, subreddits_1000, weight_225=0.75, weight_1000=0.25):
    combined_list = [(subreddit, weight_225) for subreddit in subreddits_225] + \
                    [(subreddit, weight_1000) for subreddit in subreddits_1000]
    subreddits, weights = zip(*combined_list)
    return random.choices(subreddits, weights=weights, k=1)[0]



async def handle_get_urls(batch_size: int):
    if not subreddits_top_225:
        return {"message": "Subreddit list is empty. Please fill it."}, 400
    
    urls = []
    for _ in range(batch_size):
        subreddit = random.choice(subreddits_top_225)
        subreddit_url = f"https://reddit.com/r/{subreddit.lstrip('r/')}"
        urls.append(subreddit_url)
    
    return {"urls": urls}, 200


"""
async def handle_get_urls(batch_size: int):
    if not subreddits_top_225 and not subreddits_top_1000:
        return {"message": "Subreddit lists are empty. Please fill them."}, 400
    
    urls = []
    for _ in range(batch_size):
        subreddit = weighted_choice(subreddits_top_225, subreddits_top_1000)
        subreddit_url = f"https://reddit.com/r/{subreddit.lstrip('r/')}"
        urls.append(subreddit_url)
    
    return {"urls": urls}, 200
"""
class CommentCollector:
    def __init__(self, max_items):
        self.total_items_collected = 0
        self.max_items = max_items
        self.items = []
        self.processed_ids = set()  # To track processed comment IDs
        self.lock = asyncio.Lock()
        self.stop_fetching = False
        self.logged_stop_message = False

    async def add_item(self, item, item_id):
        async with self.lock:
            if self.total_items_collected < self.max_items and item_id not in self.processed_ids:
                self.items.append(item)
                self.processed_ids.add(item_id)
                self.total_items_collected += 1
                if self.total_items_collected >= self.max_items:
                    self.stop_fetching = True
                    if not self.logged_stop_message:
                        logging.info("Maximum items collected, stopping further fetches.")
                        self.logged_stop_message = True
                return True
            return False

    def should_stop_fetching(self):
        return self.stop_fetching

async def fetch_with_proxy(session, url, collector, params=None) -> AsyncGenerator[Dict, None]:
    headers = {'User-Agent': USER_AGENT}
    retries = 0
    retry_logged = False
    while retries < MAX_RETRIES_PROXY:
        if collector.should_stop_fetching():
            logging.info("Stopping fetch_with_proxy retries as maximum items have been collected.")
            break
        try:
            async with session.get(f'{MANAGER_IP}/proxy?url={url}', headers=headers, params=params) as response:
                response.raise_for_status()
                yield await response.json()
                return
        except ClientConnectorError as e:
            if not retry_logged:
                logging.error(f"Error fetching URL {url}: Cannot connect to host {MANAGER_IP} ssl:default [{e}]")
                logging.info("Proxy servers are offline at the moment. Retrying in 10 seconds...")
                retry_logged = True
            await asyncio.sleep(10)
        except aiohttp.ClientResponseError as e:
            if e.status == 503:
                if not retry_logged:
                    logging.info("No available IPs. Retrying in 2 seconds...")
                    retry_logged = True
                await asyncio.sleep(10)
                retries += 1
            else:
                error_message = await response.json()
                if e.status == 404 and 'reason' in error_message and error_message['reason'] == 'banned':
                    logging.error(f"Error fetching URL {url}: Subreddit is banned.")
                elif e.status == 403 and 'reason' in error_message and error_message['reason'] == 'private':
                    logging.error(f"Error fetching URL {url}: Subreddit is private.")
                else:
                    logging.error(f"Error fetching URL {url}: {e.message}")
                return
        except Exception as e:
            logging.error(f"Error fetching URL {url}: {e}")
            return
    logging.error(f"Maximum retries reached for URL {url}. Skipping.")

def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def is_within_timeframe_seconds(created_utc, max_oldness_seconds, current_time):
    return (current_time - created_utc) <= max_oldness_seconds

def extract_subreddit_name(input_string):
    match = re.search(r'r/([^/]+)', input_string)
    if match:
        return match.group(1)
    return None

def post_process_item(item):
    try:
        if item.content:
            subreddit_name = extract_subreddit_name(item.url)
            if subreddit_name is None:
                return item
            segmented_subreddit_strs = segment(subreddit_name)
            segmented_subreddit_name = " ".join(segmented_subreddit_strs)
            item.content = Content(item.content + ". - " + segmented_subreddit_name + " ," + subreddit_name)
    except Exception as e:
        logging.exception(f"[Reddit post_process_item] Word segmentation failed: {e}, ignoring...")
    try:
        if item.url.startswith("https://reddit.comhttps://"):
            parts = item.url.split("https://reddit.comhttps://", 1)
            item.url = "https://" + parts[1]
    except:
        logging.warning(f"[Reddit] failed to correct the URL of item {item.url}")
    return item

async def fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time) -> AsyncGenerator[Item, None]:
    stopping_logged = False
    try:
        comments_url = f"https://www.reddit.com{post_permalink}.json"
        async for comments_json in fetch_with_proxy(session, comments_url, collector):
            if not comments_json or len(comments_json) <= 1:
                logging.info("No comments found or invalid response in fetch_comments")
                return

            comments = comments_json[1]['data']['children']
            for comment in comments:
                if collector.should_stop_fetching():
                    if not stopping_logged:
                        logging.info("Stopping fetch_comments due to max items collected")
                        stopping_logged = True
                    return

                if comment['kind'] != 't1':
                    logging.info(f"Skipping non-comment item: {comment['kind']}")
                    continue

                comment_data = comment['data']
                comment_created_at = comment_data['created_utc']

                if comment_data.get('author') == 'AutoModerator':
                    logging.info(f"Skipping AutoModerator comment: {comment_data['id']}")
                    continue

                if not is_within_timeframe_seconds(comment_created_at, max_oldness_seconds, current_time):
                    continue

                comment_content = comment_data.get('body', '[deleted]')
                comment_author = comment_data.get('author', '[unknown]')
                comment_url = f"https://reddit.com{comment_data['permalink']}"
                comment_id = comment_data['name']

                if not is_valid_item(comment_content, comment_url, min_post_length):
                    logging.info(f"Skipping invalid comment: {comment_data['id']} with content '{comment_content}'")
                    continue

                item = Item(
                    content=Content(comment_content),
                    author=Author(hashlib.sha1(bytes(comment_author, encoding="utf-8")).hexdigest()),
                    created_at=CreatedAt(format_timestamp(comment_created_at)),
                    domain=Domain("reddit.com"),
                    url=Url(comment_url),
                )

                if await collector.add_item(item, comment_id):
                    logging.info(f"New valid comment found: {item}")
                    yield item
    except GeneratorExit:
        logging.info("GeneratorExit received in fetch_comments, exiting gracefully.")
        raise
    except Exception as e:
        logging.error(f"Error in fetch_comments: {e}")

async def fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, limit=100, after=None) -> AsyncGenerator[Item, None]:
    stopping_logged = False
    try:
        params = {
            'limit': limit,
            'raw_json': 1
        }
        if after:
            params['after'] = after

        if not subreddit_url.endswith('.json'):
            subreddit_url_with_limit = f"{subreddit_url.rstrip('/')}/.json"
        else:
            subreddit_url_with_limit = subreddit_url

        query_params = '&'.join([f'{key}={value}' for key, value in params.items()])
        final_url = f"{subreddit_url_with_limit}?{query_params}"

        async for response_json in fetch_with_proxy(session, final_url, collector):
            if not response_json or 'data' not in response_json or 'children' not in response_json['data']:
                logging.info("No posts found or invalid response in fetch_posts")
                return

            posts = response_json['data']['children']
            for post in posts:
                if collector.should_stop_fetching():
                    if not stopping_logged:
                        logging.info("Stopping fetch_posts due to max items collected")
                        stopping_logged = True
                    return

                post_kind = post.get('kind')
                post_info = post.get('data', {})

                if post_kind != 't3':
                    logging.info(f"Skipping non-post item: {post_kind}")
                    continue

                post_permalink = post_info.get('permalink', None)
                post_created_at = post_info.get('created_utc', 0)

                # Fetch comments for the post regardless of whether the post meets the criteria
                if post_permalink:
                    async for comment in fetch_comments(session, post_permalink, collector, max_oldness_seconds, min_post_length, current_time):
                        yield comment

                # Check if the post itself meets the criteria
                if is_within_timeframe_seconds(post_created_at, max_oldness_seconds, current_time):
                    post_content = post_info.get('selftext', '[deleted]')
                    post_author = post_info.get('author', '[unknown]')
                    post_url = f"https://reddit.com{post_permalink}"
                    post_id = post_info['name']

                    if not is_valid_item(post_content, post_url, min_post_length):
                        logging.info(f"Skipping invalid post: {post_id} with content '{post_content}'")
                        continue

                    item = Item(
                        title=Title(post_info.get('title', '[deleted]')),
                        content=Content(post_content),
                        author=Author(hashlib.sha1(bytes(post_author, encoding="utf-8")).hexdigest()),
                        created_at=CreatedAt(format_timestamp(post_created_at)),
                        domain=Domain("reddit.com"),
                        url=Url(post_url),
                    )

                    if await collector.add_item(item, post_id):
                        logging.info(f"New valid post found: {item}")
                        yield item

            new_after = response_json['data'].get('after')
            if new_after:
                logging.info(f"Fetched page with after={new_after}")
            else:
                logging.info("Fetched page but no after parameter found, this might be the last page")
            yield new_after
    except GeneratorExit:
        logging.info("GeneratorExit received in fetch_posts, exiting gracefully.")
        raise
    except Exception as e:
        logging.error(f"Error in fetch_posts: {e}")
        yield None

def is_valid_item(content, url, min_post_length):
    if len(content.strip()) < min_post_length or content.strip().startswith('http') or \
       content == "[deleted]" or url.startswith("https://reddit.comhttps:") or not ("reddit.com" in url):
        return False
    return True

async def limited_fetch(semaphore, session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, nb_subreddit_attempts, post_limit) -> List[Item]:
    async with semaphore:
        items = []
        try:
            after = None
            items_fetched = 0
            page_number = 1
            last_after = None
            while items_fetched < 1000 and not collector.should_stop_fetching():
                logging.info(f"Fetching page {page_number} for subreddit {subreddit_url} with after={after}")
                async for result in fetch_posts(session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, post_limit, after):
                    if isinstance(result, str):
                        last_after = after
                        after = result
                    elif result is None:
                        break
                    else:
                        items.append(result)
                        items_fetched += 1
                if not after or after == last_after:
                    logging.info(f"No more pages to fetch for subreddit {subreddit_url} or after parameter did not change.")
                    break
                page_number += 1
        except GeneratorExit:
            logging.info("GeneratorExit received inside limited_fetch, exiting gracefully.")
            raise
        except Exception as e:
            logging.error(f"Error inside limited_fetch: {e}")
        return items

async def query(parameters: Dict) -> AsyncGenerator[Item, None]:
    max_oldness_seconds = parameters.get('max_oldness_seconds')
    maximum_items_to_collect = parameters.get('maximum_items_to_collect', 25)  # Default to 25 if not provided
    min_post_length = parameters.get('min_post_length')
    batch_size = parameters.get('batch_size', 5)
    nb_subreddit_attempts = parameters.get('nb_subreddit_attempts', 5)
    post_limit = parameters.get('post_limit', 100)  # Limit for the number of posts per subreddit

    logging.info(f"[Reddit] Input parameters: max_oldness_seconds={max_oldness_seconds}, "
                 f"maximum_items_to_collect={maximum_items_to_collect}, min_post_length={min_post_length}, "
                 f"batch_size={batch_size}, nb_subreddit_attempts={nb_subreddit_attempts}, post_limit={post_limit}")

    collector = CommentCollector(maximum_items_to_collect)
    current_time = datetime.now(timezone.utc).timestamp()

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_TASKS)) as session:
        try:
            urls_response, status = await handle_get_urls(batch_size)
            if status != 200:
                logging.error("Failed to get subreddit URLs")
                return

            subreddit_urls = urls_response['urls']

            semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
            tasks = [limited_fetch(semaphore, session, subreddit_url, collector, max_oldness_seconds, min_post_length, current_time, nb_subreddit_attempts, post_limit) for subreddit_url in subreddit_urls]

            all_tasks = asyncio.gather(*tasks)

            try:
                all_items = await all_tasks

                for items in all_items:
                    for item in items:
                        item = post_process_item(item)
                        logging.info(f"Found item: {item}")
                        yield item
            except GeneratorExit:
                logging.info("GeneratorExit received in query, canceling all tasks.")
                all_tasks.cancel()
                raise
        except Exception as e:
            logging.error(f"Error in query: {e}")
        finally:
            logging.info("Cleaning up: closing session.")