import asyncio
import logging
import site
import yaml
import functools
import voluptuous as vol

from icalendar import Calendar, Event, vCalAddress, vText
from datetime import datetime, timedelta
from pathlib import Path

from .. import fhem, generic, utils
from fhempy.lib.abfall.waste_collection_schedule import Customize, SourceShell, CollectionAggregator

class abfall(generic.FhemModule):
    def __init__(self, logger):
        super().__init__(logger)

        attr_config = {
            "Updateinterval": {
                "default": 60,
                "format": "int",
                "help": "Update interval in Minutes, default is 60.",
                "function": "restart_update_loop"
            },
            "OnlyNextReading": {
                "default": 0, 
                "format": "int",
                "options": "0,1",
                "function": "set_update",
                "help": "Display only Reading for next appointment if set to 1"
            },
            "UpdateSourcesInterval": {
                "default": 7,
                "format": "int",
                "help": "Update Interval for Sources in Days, default is 7."
            },
            "Sources": {
                "default": "{}",
                "format": "yaml",
                "options": "textField-long",
                "help": "Define Sources",
                "function": "set_update"
            },
            "ExcludedWastetypes": {
                "default": None,
                "format": "str",
                "options": "textField-long",
                "help": "Define Excluded Wastetypes (Value -> Reading_text) <br> \
                Example: Restabfall 1100l 2 wö,Restabfall 1100l wö",
                "function": "set_update"
            },
            "WeekdayMapping": {
                "default": "Montag,Dienstag,Mittwoch,Donnerstag,Freitag,Samstag,Sonntag",
                "format": "string",
                "help": "Define Weekday Mapping. Week starts with Monday <br> \
                    Example: Montag,Dienstag,Mittwoch,Donnerstag,Freitag,Samstag,Sonntag",
                "function": "set_update"
            },
            "DaySwitchTime": {
                "default": "12:00",
                "format": "string",
                "help": "Define Switchtime to Nextday for Wastetypes and Next are picked up today. Format HH:MM",
                "function": "restart_dayswitch_loop"
            }            
        }
        self.set_attr_config(attr_config)

        set_config = {
            "update": {}
        }
        self.set_set_config(set_config)
        self.addtopath()
        self.sources = ''
        self.include_today = True

    # FHEM FUNCTION
    async def Define(self, hash, args, argsh):
        await super().Define(hash, args, argsh)
        if len(args) > 3:
            return "Usage: define define fhempy abfall"
        
        # Set Initial Status
        await fhem.readingsBeginUpdate(hash)
        await fhem.readingsBulkUpdateIfChanged(hash, "state", "initialized")
        await fhem.readingsEndUpdate(hash, 1)
        
        # Set to update Events on Startup
        self.fetchneeded = True
        
        #await self.set_update(hash)
        self.logger.debug("Called Update for Updating Events after Startup")
        
        # Create Loop for Dayswitch
        self.dayswitch_loop = self.create_async_task(self.create_dayswitch_loop())
        
        # Create Loop for Updates
        self.update_loop = self.create_async_task(self.create_update_loop())
        

        
        #Set State Format
        stateformat = await fhem.AttrVal(self.hash["NAME"], "stateFormat", "nostateformat")
        self.logger.debug(self.hash["NAME"])
        if stateformat == "nostateformat":
            await fhem.CommandAttr(
                self.hash, self.hash["NAME"] + " stateFormat {if(ReadingsVal(\$name,'next_days',0) == 1) {return ReadingsVal(\$name,'next_text',0).' wird morgen abgeholt!';}\n\
elsif(ReadingsVal(\$name,'next_days',0) == 0){return ReadingsVal(\$name,'next_text',0).' wird heute abgeholt!!!';}\n\
else {return ReadingsVal(\$name,'next_text',0).' wird in '.ReadingsVal(\$name,'next_days',0).' Tagen abgeholt';}}"
            )
        
        #Set Icon if no Icon
        icon = await fhem.AttrVal(self.hash["NAME"], "icon", "noicon")
        if icon == "noicon":
            await fhem.CommandAttr(
                self.hash, self.hash["NAME"] + " icon Abfalltonne"
            )   
    
    # Set Functions
    async def create_update_loop(self):
        while True:
            await self.set_update(self.hash)
            await asyncio.sleep(self._attr_Updateinterval * 60)
    
    async def restart_update_loop(self, parameter = ''):
        self.cancel_async_task(self.update_loop)
        self.update_loop = self.create_async_task(self.create_update_loop())
        
    
    async def create_dayswitch_loop(self):
        #get time and date
        n = datetime.now()
        today = datetime.now().date()
        
        # Get Seconds to Dayswitchtime
        dayswitchtime = self._attr_DaySwitchTime
        time = datetime.strptime(dayswitchtime, '%H:%M').time()
        dayswitchdate = datetime.combine(today, time)
        
        # If Dayswitch is in the past add one Day
        if (dayswitchdate - n).total_seconds() < 0:
            dayswitchdate = dayswitchdate + timedelta(days=1)
            self.include_today = False
            pass
        else:
            self.include_today = True
            pass
        secondstodayswitch = int((dayswitchdate - n).total_seconds())
        
        # Get Seconds to Midnight
        time = datetime.strptime("23:59:59", '%H:%M:%S').time()
        midnight = datetime.combine(today, time)
        secondstomidnight = int((midnight - n).total_seconds())

        if secondstomidnight < secondstodayswitch:
            self.logger.debug("next is Midnight")
            await asyncio.sleep(secondstomidnight)
            self.include_today = True
            self.set_update(self.hash)
            pass
        else:
            self.logger.debug("next is dayswitch")
            await asyncio.sleep(secondstodayswitch)
            self.include_today = False
            self.set_update(self.hash)
            pass
    
    async def restart_dayswitch_loop(self, parameter = ''):
        self.cancel_async_task(self.dayswitch_loop)
        self.dayswitch_loop = self.create_async_task(self.create_dayswitch_loop())
        await self.set_update(self.hash)
               
    async def set_update(self, hash, parameter = ''):
        
        await fhem.readingsSingleUpdate(hash, "state", "updating", 1)

        #Check if Source Defninition has changed
        await self.checksourceyml()
        
        #Check if next fetchtime arrived        
        await self.checknextfetch()

        # Update Events if needed
        if self.fetchneeded:
            self.create_async_task(self.fetchevents())
            self.logger.info("Update for Events started")
            return
                
        # Create Sourcelocation to set Location to Events
        await self.createsourcelocation()
        
        #Get Excluded Wastetypes form Attributes
        excludewastetypes = set(self._attr_ExcludedWastetypes.split(","))

        try:
            #Create Aggregator for all Sources
            aggregator = CollectionAggregator(self.api.shells)

            #Update Next Reading
            self.logger.info("Update Next Reading")
            upcoming = aggregator.get_upcoming_group_by_day(count=1, include_today=self.include_today, exclude_types=excludewastetypes)
            readingname=str("next")
            self.create_async_task(self.update_readings(hash, upcoming, readingname))

            #Update Singele Waste Readings
            self.logger.debug ("Attr OnlynextReading")
            if self._attr_OnlyNextReading == 0:
                self.logger.info("Update single waste readings")
                upcoming = aggregator.get_upcoming(leadtime=30, include_today=self.include_today, exclude_types=excludewastetypes)
                self.create_async_task(self.update_readings(hash, upcoming))
                pass
            elif (yaml.safe_load(await fhem.ReadingsVal(self.hash["NAME"], ".readingnames", ""))) != set() :
                await self.cleanupreadings(readingnames= set({'none'}))
                await fhem.readingsSingleUpdate(self.hash, ".readingnames", yaml.dump(set()), 1)
                pass
        except:
            await fhem.readingsSingleUpdate(hash, "state", "update readings failed", 1)
            self.logger.error("Error in Updaiting Events")
            
        self.logger.info("Update readings finishid")
        await fhem.readingsSingleUpdate(hash, "state", "update finishid", 1)
        
        upcoming = aggregator.get_upcoming()
        ical = await self.createical(upcoming)
        #await fhem.readingsSingleUpdate(hash, "iCalendar", ical, 1)
        return 
    
    # Additional Functions
    async def update_readings(self, hash, upcoming, readingname = ''):
        #Check if Name for Reading is Speficed
        if not bool(readingname):
            createname = True
            pass
        else:
            createname = False
            pass
        
        # Specify Set to dertmine Duplicate Eventtyple -> Multiple Date for Wastetype
        duplicates = set()
        
        #Get Weekdaymapping form Attributes
        try:
            weekday = (self._attr_WeekdayMapping).split(",")
        except:
            self.logger.error("Wrong Weekdaymapping use defaults")
            weekday = ("Montag,Dienstag,Mittwoch,Donnerstag,Freitag,Samstag,Sonntag").split(",")
        # Loop trough the given event.
        for event in upcoming:
            readingsupdate = {}
            # Handle multiple Event per Day (Only used for next)
            if hasattr(event, 'types'):
                for type in event.types:
                    if not bool(readingsupdate):
                        try:
                            location = self.sourcelocation[type]["location"]
                            source = self.sourcelocation[type]["source"]
                        except:
                            self.logger.warning("No Location found")
                            location = "not found"
                            source = "not found"   
                        readingsupdate["date"] = event._date.strftime("%d.%m.%Y")
                        readingsupdate["days"] = event.daysTo
                        readingsupdate["weekday"] = weekday[event._date.weekday()]
                        readingsupdate["header"] = self.remove_umlaut(type)
                        readingsupdate["desription"] = type
                        readingsupdate["location"] = location 
                        readingsupdate["text"] = type
                        readingsupdate["source"] = source
                        pass
                    else:
                        try:
                            location = self.sourcelocation[type]["location"]
                            source = self.sourcelocation[type]["source"]
                        except:
                            self.logger.warning("No Location found")
                            location = "not found"
                            source = "not found"  
                        readingsupdate["header"] = readingsupdate["header"] + " und " + self.remove_umlaut(type)
                        readingsupdate["desription"] = readingsupdate["desription"] + " und " + type
                        readingsupdate["location"] = readingsupdate["location"] + " und " + location 
                        readingsupdate["text"] = readingsupdate["text"] + " und " + type
                        readingsupdate["source"] = source
                        pass
                pass
            # Handle Single Event per Day
            else:
                if event.type not in duplicates:
                    try:
                        location = self.sourcelocation[event.type]["location"]
                        source = self.sourcelocation[event.type]["source"]
                    except:
                        self.logger.warning("No Location found")
                        location = "not found"
                        source = "not found"  
                    readingsupdate["date"] = event._date.strftime("%d.%m.%Y")
                    readingsupdate["days"] = event.daysTo
                    readingsupdate["weekday"] = weekday[event._date.weekday()]
                    readingsupdate["header"] = self.remove_umlaut(event.type)
                    readingsupdate["desription"] = event.type
                    readingsupdate["location"] = location 
                    readingsupdate["text"] = event.type
                    readingsupdate["source"] = source
                    pass
                pass
            if bool(readingsupdate):
                readingsupdate["desription"] = readingsupdate["desription"] + " nicht vergessen!"
                if createname:
                    readingname = (readingsupdate["header"]).replace(" ", "")
                    # Add Event to List to get only next Date
                    duplicates.add(event.type)
                    pass
                await fhem.readingsBeginUpdate(hash)
                await fhem.readingsBulkUpdate(hash, readingname, readingsupdate["header"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_date"), readingsupdate["date"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_days"), readingsupdate["days"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_description"), readingsupdate["desription"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_location"), readingsupdate["location"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_text"), readingsupdate["text"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_weekday"), readingsupdate["weekday"])
                await fhem.readingsBulkUpdate(hash, (readingname + "_source"), readingsupdate["source"])
                await fhem.readingsEndUpdate(hash, 1)
                pass
        
        if not duplicates == set():
            await self.cleanupreadings(duplicates)
            await fhem.readingsSingleUpdate(self.hash, ".readingnames", yaml.dump(duplicates), 1)
        
    def addtopath(self):
    # add module directory to path to Source Files
        package_dir = Path(__file__).resolve().parents[0]
        site.addsitedir(str(package_dir))
        self.logger.debug("Add Directory " + str(package_dir) + " to Path")
    
    def createwasteapi(self, sourceyml):
        #### Create Shema
        CONF_SOURCES = "sources"
        CONF_SOURCE_NAME = "name"
        CONF_SOURCE_ARGS = "args"  # source arguments
        CONF_SOURCE_CALENDAR_TITLE = "calendar_title"
        CONF_SEPARATOR = "separator"

        CONF_CUSTOMIZE = "customize"
        CONF_TYPE = "type"
        CONF_ALIAS = "alias"
        CONF_SHOW = "show"
        CONF_ICON = "icon"
        CONF_PICTURE = "picture"
        CONF_USE_DEDICATED_CALENDAR = "use_dedicated_calendar"
        CONF_DEDICATED_CALENDAR_TITLE = "dedicated_calendar_title"

        CUSTOMIZE_CONFIG = vol.Schema(
            {
                vol.Optional(CONF_TYPE),
                vol.Optional(CONF_ALIAS),
                vol.Optional(CONF_SHOW),
                vol.Optional(CONF_ICON),
                vol.Optional(CONF_PICTURE),
                vol.Optional(CONF_USE_DEDICATED_CALENDAR),
                vol.Optional(CONF_DEDICATED_CALENDAR_TITLE),
            }
        )

        SOURCE_CONFIG = vol.Schema(
            {
                vol.Required(CONF_SOURCE_NAME),
                vol.Required(CONF_SOURCE_ARGS),
                vol.Optional(CONF_CUSTOMIZE, default=vol.All([CUSTOMIZE_CONFIG])),
                vol.Optional(CONF_SOURCE_CALENDAR_TITLE),
            }
        )

        api = WasteCollectionApi(
                separator=",",
            )

        customize = {}
        customize["default"] = Customize(
            waste_type="none",
            alias="",
            show=True,
            icon="",
            picture="",
            use_dedicated_calendar=False,
            dedicated_calendar_title=False,
            )
        for wastesource in sourceyml["sources"]:
            api.add_source_shell(
                source_name=(wastesource["name"]),
                customize=customize,
                calendar_title=(wastesource["location"]),
                source_args=(wastesource["args"]),
                )
        return api

    def remove_umlaut(self, string):
        """
        Removes umlauts from strings and replaces them with the letter+e convention
        :param string: string to remove umlauts from
        :return: unumlauted string
        """
        u = 'ü'.encode()
        U = 'Ü'.encode()
        a = 'ä'.encode()
        A = 'Ä'.encode()
        o = 'ö'.encode()
        O = 'Ö'.encode()
        ss = 'ß'.encode()
        plus = '+'.encode()

        string = string.encode()
        string = string.replace(u, b'ue')
        string = string.replace(U, b'Ue')
        string = string.replace(a, b'ae')
        string = string.replace(A, b'Ae')
        string = string.replace(o, b'oe')
        string = string.replace(O, b'Oe')
        string = string.replace(ss, b'ss')
        string = string.replace(plus, b'')

        string = string.decode('utf-8')
        return string

    async def createical(self, events):
        cal = Calendar()
        # Some properties are required to be compliant
        cal.add('prodid', '-//My calendar product//example.com//')
        cal.add('version', '2.0')

        for event in events:
            description = (event.type).encode(encoding='utf-8')
            uid = self.remove_umlaut( event.date.strftime("%Y%m%d") + event.type).replace(" ", "")
            try:
                location = self.sourcelocation[event.type]["location"]
                source = self.sourcelocation[event.type]["source"]
            except:
                self.logger.warning("No Location found")
                location = "not found"
                source = "not found" 
            calevent = Event()
            calevent.add('summary', description)
            calevent.add('description', description)
            calevent.add('dtstart', event.date)
            calevent.add('dtend', event.date)
            calevent.add('uid', uid)
            calevent['location'] = location
            organizer = vCalAddress(source)
            organizer.params['name'] = source
            calevent['organizer'] = organizer
            cal.add_component(calevent)

        return cal.to_ical().decode("utf-8").replace('\r\n', '\n').strip()

    async def fetchevents(self):
        # Create wasteapi
        try:
            self.sources = yaml.safe_load(self._attr_Sources)
            self.api =  self.createwasteapi(self.sources)
            self.logger.info("New Api Created")
        

        # Fetch Events from API
            refreshtime = await utils.run_blocking(functools.partial(self.api._fetch))
            self.fetchneeded = False
            self.logger.info("Fetched new Events")
            await fhem.readingsSingleUpdate(self.hash, "_lasteventupdate", refreshtime, 1)
        
            # Call update for create new Events
            await self.set_update(self.hash)
        except:
            await fhem.readingsSingleUpdate(self.hash, "state", "no sources found or definition is wrong", 1)
            await fhem.readingsSingleUpdate(self.hash, "_lasteventupdate", datetime.now(), 1)
            pass
    
    async def checksourceyml(self):
        newsources = yaml.safe_load(self._attr_Sources)
        ischanged = newsources == self.sources
        if not ischanged:
            self.logger.info("Sources changed")
            self.fetchneeded = True
            pass

    async def checknextfetch(self):
        lasteventupdate = await fhem.ReadingsVal(self.hash["NAME"], "_lasteventupdate", "")
        try:
            datetime_object = datetime.strptime(lasteventupdate, '%Y-%m-%d %H:%M:%S')
            nextfetch = datetime_object + timedelta(days=self._attr_UpdateSourcesInterval)
        except:
            self.fetchneeded = True
            return
        if datetime.now() > nextfetch:
            self.logger.info("update needed")
            self.fetchneeded = True
            pass
        else:
            self.logger.debug("no update needed")
            pass
        pass    
 
    async def createsourcelocation(self):
        # Create Dict and get Wasttypes for every Sourceshell
        try:
            self.sourcelocation = dict()
            for shellindex, sourceshell in enumerate(self.api.shells):
                wastetypes = CollectionAggregator([self.api.get_shell(shellindex)])
                for type in wastetypes.types:
                    types = {
                    "location" : sourceshell.calendar_title,
                    "source" : sourceshell.title,
                    "shell" : shellindex
                    }
                    self.sourcelocation[type] = types
                    self.logger.info("Add Wastetype " + type + " to Sourcelocation")
            self.logger.debug(self.sourcelocation)
        except:
            self.logger.warning("creation of sourcelocation failed")

    async def cleanupreadings(self, readingnames = set(), singlereading = ""):
        if readingnames != set ():
            self.logger.info("Cleanup Readings")
            oldreadings = yaml.safe_load(await fhem.ReadingsVal(self.hash["NAME"], ".readingnames", ""))
            if isinstance(oldreadings, set):
                to_delete = oldreadings.difference(readingnames)
                self.logger.debug("readings to be deleted " + str(to_delete))
                for reading in to_delete:
                    self.logger.debug("delete readings for " + reading)
                    readingname = (self.remove_umlaut(reading)).replace(" ", "")
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + readingname + "*")
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_date"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_days"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_description"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_location"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_text"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_weekday"))
                    await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_source"))
                pass         
        elif singlereading != "":
            self.logger.debug("delete readings for " + reading)
            readingname = (self.remove_umlaut(singlereading)).replace(" ", "")
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + readingname + "*")
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_date"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_days"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_description"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_location"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_text"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_weekday"))
            await fhem.CommandDeleteReading(self.hash, self.hash["NAME"] + " " + (readingname + "_source"))
            pass
        else:
            self.logger.debug("No Reading to Cleanup given")
            pass
   
class WasteCollectionApi:
    def __init__(
        self, separator
    ):
        self._source_shells = []
        self._separator = separator
        self.logger = logging.getLogger(__name__)

    @property
    def separator(self):
        """Separator string, used to separator waste types."""
        return self._separator

    def add_source_shell(
        self,
        source_name,
        customize,
        source_args,
        calendar_title,
        ):
        
        self._source_shells.append(
            SourceShell.create(
                source_name=source_name,
                customize=customize,
                source_args=source_args,
                calendar_title=calendar_title,
            )
        )
        
    def _fetch(self, *_):
        try:
            for shell in self._source_shells:
                shell.fetch()
                refreshtime = shell.refreshtime
        except:
            self.logger.error("No Source Shell Found")  
        return refreshtime   


    @property
    def shells(self):
        return self._source_shells

    def get_shell(self, index):
        return self._source_shells[index] if index < len(self._source_shells) else None