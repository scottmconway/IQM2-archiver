#!/usr/bin/env python3

import argparse
import datetime
import json
import os
from urllib.parse import parse_qs, urlparse

import bs4
import requests
from sqlalchemy import (
    DDL,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    create_engine,
    event,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Mapped,
    declarative_base,
    mapped_column,
    relationship,
    sessionmaker,
)

Base = declarative_base()

# Association tables
voting_seconders = Table(
    "votingSeconders",
    Base.metadata,
    Column("voting_id", Integer, ForeignKey("resolutionVotes.id"), primary_key=True),
    Column("person_id", Integer, ForeignKey("people.id"), primary_key=True),
)


class Resolution(Base):
    __tablename__ = "resolutions"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    type = Column(String, nullable=False)
    title = Column(String, nullable=False)
    department = Column(String, nullable=False)
    category = Column(String, nullable=False)
    title = Column(String, nullable=False)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)

    attachments = relationship(
        "ResolutionAttachment",
        back_populates="resolution",
        cascade="all, delete-orphan",
    )
    meetings = relationship(
        "ResolutionMeeting", back_populates="resolution", cascade="all, delete-orphan"
    )
    customSections = relationship(
        "ResolutionCustomSection",
        back_populates="resolution",
        cascade="all, delete-orphan",
    )
    functions = relationship(
        "ResolutionFunction", back_populates="resolution", cascade="all, delete-orphan"
    )
    votes = relationship(
        "ResolutionVote", back_populates="resolution", cascade="all, delete-orphan"
    )
    sponsors = relationship(
        "ResolutionSponsor", back_populates="resolution", cascade="all, delete-orphan"
    )


class ResolutionAttachment(Base):
    __tablename__ = "resolutionAttachments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    path = Column(String, nullable=False)
    title = Column(String, nullable=False)
    resolution = relationship("Resolution", back_populates="attachments")


class ResolutionFunction(Base):
    __tablename__ = "resolutionFunctions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    name = Column(String, nullable=False)
    resolution = relationship("Resolution", back_populates="functions")


class ResolutionCustomSection(Base):
    __tablename__ = "resolutionCustomSections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    name = Column(String, nullable=False)
    content = Column(String, nullable=False)
    resolution = relationship("Resolution", back_populates="customSections")


class ResolutionMeeting(Base):
    __tablename__ = "resolutionMeetings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    meetingId = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.min, nullable=False)
    resolution = relationship("Resolution", back_populates="meetings")


class ResolutionVote(Base):
    __tablename__ = "resolutionVotes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    result = Column(String, nullable=False)
    mover_id = Column(Integer, ForeignKey("people.id"), nullable=False)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    # seconders = relationship("Person", secondary=voting_seconders, back_populates="seconder_votings")
    resolution = relationship("Resolution", back_populates="votes")
    mover = relationship(
        "Person", back_populates="moved_votes", foreign_keys=[mover_id]
    )
    person_votes = relationship(
        "PersonVote", back_populates="resolution_vote", cascade="all, delete-orphan"
    )


class ResolutionSponsor(Base):
    __tablename__ = "resolutionSponsors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_id = Column(Integer, ForeignKey("resolutions.id"), nullable=False)
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False)

    resolution = relationship("Resolution", back_populates="sponsors")
    person = relationship("Person", back_populates="sponsorships")


class Person(Base):
    __tablename__ = "people"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    # people's titles can change over time!
    # title = Column(String, nullable=True)
    sponsorships = relationship("ResolutionSponsor", back_populates="person")
    moved_votes = relationship(
        "ResolutionVote", back_populates="mover", foreign_keys=[ResolutionVote.mover_id]
    )
    # seconder_votes = relationship("ResolutionVote", secondary=voting_seconders, back_populates="seconders")
    # votes = relationship("PersonVote", secondary=voting_ayes, back_populates="ayes")


class PersonVote(Base):
    """
    An association between a person, the voting event, and their voting type.
    Composite primary key of person.id and resolutionVotes.id,
    since a person can only vote once per voting event.
    """

    __tablename__ = "personVotes"

    person_id = Column(
        Integer, ForeignKey("people.id"), primary_key=True, nullable=False
    )
    resolution_vote_id = Column(
        Integer, ForeignKey("resolutionVotes.id"), primary_key=True, nullable=False
    )
    vote_type_id = Column(Integer, ForeignKey("voteTypes.id"), nullable=False)
    resolution_vote = relationship("ResolutionVote", back_populates="person_votes")


class VoteType(Base):
    """
    used to hold types of votes, such as "aye", "nay", "abstain", etc.
    """

    __tablename__ = "voteTypes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


view_ddl = DDL(
    """
CREATE VIEW resolution_votes_view AS
SELECT
   rv.resolution_id,
   vt.name AS vote_type,
    GROUP_CONCAT(p.name, ', ') AS voter_names
FROM resolutionVotes rv
JOIN personVotes pv ON rv.id = pv.resolution_vote_id
JOIN voteTypes vt ON pv.vote_type_id = vt.id
JOIN people p ON pv.person_id = p.id
GROUP BY
    rv.resolution_id,
    vt.name;
"""
)
## Automatically create the view when the metadata is created.
event.listen(Base.metadata, "after_create", view_ddl)

# Drop the view if dropping the metadata.
drop_view_ddl = DDL("DROP VIEW IF EXISTS resolution_votes_view")
event.listen(Base.metadata, "before_drop", drop_view_ddl)


def safe_find(soup, name, **kwargs):
    element = soup.find(name, **kwargs)
    if element is None:
        raise Exception(f"Could not find element '{name}' with kwargs {kwargs}")
    return element


class IqmScraper:
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    ERR_RES = [
        "The requested Document could not be retrieved.",
        "Access Denied You do not have permissions to view",
    ]

    EMPTY_DISCUSSION_TEXT = "\nDiscussion\n\n\n\nAdd Comment\n\n\nType in your comments here\n\xa0Add Comment\xa0\nComment to board only \n\n\n\n\n\n"

    WELL_KNOWN_SECTION_NAMES = [
        "Information",
        "Attachments",
        "Body",
        "Meeting History",
        "Discussion",
    ]

    def __init__(self, base_url: str, database_session):
        self.legifile_url = f"{base_url}/Citizens/Detail_LegiFile.aspx"
        self.session = requests.Session()
        self.session.headers.update(IqmScraper.HEADERS)
        self.database_session = database_session

        # store known voting types / known people
        self.voting_types = self.database_session.query(VoteType).all()
        self.people_name_to_id = dict()
        self.refresh_people()

        self.custom_vote_type_name_to_id = dict()
        self.refresh_custom_vote_types()

    def add_person(self, name):
        """
        Add an entry to the person table by name
        """
        self.database_session.add(Person(name=name))

    def refresh_people(self):
        self.people_name_to_id = {
            person.name: person.id
            for person in self.database_session.query(Person).all()
        }

    def add_custom_vote_type(self, name):
        self.database_session.add(VoteType(name=name))

    def refresh_custom_vote_types(self):
        self.custom_vote_type_name_to_id = {
            vote_type.name: vote_type.id
            for vote_type in self.database_session.query(VoteType).all()
        }

    def get_resolution(
        self, resolution_id: int, include_body: bool = True
    ) -> dict | None:
        page_res = self.session.get(self.legifile_url, params={"ID": resolution_id})

        if any(err in page_res.text for err in IqmScraper.ERR_RES):
            print(f"err response for {resolution_id}")
            return

        soup = bs4.BeautifulSoup(page_res.text, "html.parser")

        resolution = Resolution(
            id=resolution_id,
            name=safe_find(soup, "div", id="ContentPlaceholder1_lblResNum").text,
            type=safe_find(soup, "div", id="ContentPlaceholder1_lblLegiFileType").text,
            title=safe_find(soup, "h1", id="ContentPlaceholder1_lblLegiFileTitle").text,
            body=None,
        )

        # get custom sections
        # ex. 'Financial Impact'
        # https://amherstny.iqm2.com/Citizens/Detail_LegiFile.aspx?ID=29176
        sections = soup.find_all(class_="LegiFileSection")
        for section in sections:
            section_name = section.find("h4").text

            # only scrape sections that we don't manually parse later
            if section_name not in IqmScraper.WELL_KNOWN_SECTION_NAMES:
                section_content = (
                    section.find(class_="LegiFileSectionContents")
                    .text.strip()
                    .replace("\xa0", "")
                )
                custom_section = ResolutionCustomSection(
                    name=section_name, content=section_content
                )
                resolution.customSections.append(custom_section)

        # get Information section
        information_table = safe_find(soup, "table", id="tblLegiFileInfo")
        assert isinstance(information_table, bs4.element.Tag)

        table_headers = information_table.find_all("th")
        table_data = information_table.find_all("td")

        if len(table_headers) != len(table_data):
            print(f"error parsing information table for resolution {resolution_id}")

        for index in range(len(table_headers)):
            # 'Department:' -> 'department'
            header_name = table_headers[index].text.replace(":", "").lower()
            data_value = table_data[index].text

            # data values may sometimes be empty strings
            if data_value:
                # add well-known headers to the resolution entry
                if header_name == "department":
                    resolution.department = data_value
                elif header_name == "category":
                    resolution.category = data_value
                elif header_name == "functions":
                    resolution.functions = [
                        ResolutionFunction(name=i) for i in data_value.split(", ")
                    ]
                elif header_name == "sponsors":
                    sponsors = [sponsor for sponsor in data_value.split(", ")]
                    # TODO titles are still present!
                    # titles may be any number of words

                    # to find the person, we could search for people
                    # from the right-most word, moving back one word at a time
                    # eg. search for "omalley", then "jill omalley", then "councilmember jill omalley"
                    # if we have a full match, we could even save the first part as a "title"
                    #
                    # This seems intensive on the database...

                    # people table should be populated before this search would be effective
                    resolution.sponsors = []

        # get Attachments section
        attachment_section = soup.find("div", id="ContentPlaceholder1_divDownloads")
        if attachment_section:
            assert isinstance(attachment_section, bs4.element.Tag)
            for a_tag in attachment_section.find_all("a"):

                attachment = ResolutionAttachment(
                    path=a_tag.attrs["href"], title=a_tag.text
                )
                resolution.attachments.append(attachment)

        # get Body section
        # they _really_ like to use \xa0 for padding
        # note - this kills _some_ formatting!

        if include_body:
            body_section = soup.find("div", id="divBody")
            if body_section:
                assert isinstance(body_section, bs4.element.Tag)
                resolution.body = safe_find(
                    body_section, "div", class_="LegiFileSectionContents"
                ).text.replace("\xa0", "")
                # this may strip images and other garbage from the body (and that's ok)

        # TODO `get_body` should be replaced with something like "get_text_sections"

        # get Discussion section
        discussion_section = safe_find(
            soup, "div", id="ContentPlaceholder1_divDiscussion"
        )
        if discussion_section.text != IqmScraper.EMPTY_DISCUSSION_TEXT:
            pass
            # this is a stub since I have yet
            # to see this feature used in the wild

        # get Meeting History / Voting results
        # records will be from oldest to newest
        meeting_history = soup.find("table", class_="LayoutTable MeetingHistory")

        if meeting_history:
            assert isinstance(meeting_history, bs4.element.Tag)
            voting_records = meeting_history.find_all("table", class_="VoteRecord")
            header_rows = meeting_history.find_all(
                "tr", class_="HeaderRow HistorySection"
            )

            for index in range(len(voting_records)):
                header_row = header_rows[index]
                voting_record = voting_records[index]

                history_item = {"meeting": {}, "voting": {}}

                # get header row info
                # eg. "Town Board - Regular
                meeting_type = " - ".join(
                    [
                        header_row.find("td", class_="Group").text,
                        header_row.find("td", class_="Type").text,
                    ]
                )
                meeting_td = header_row.find("td", class_="Date")
                meeting_datetime = datetime.datetime.strptime(
                    meeting_td.text.split("\xa0")[0], "%b %d, %Y %I:%M %p"
                )

                # try to find the meeting ID (best effort)
                meeting_id = None
                for a_tag in meeting_td.find_all("a"):
                    a_tag.attrs["href"]
                    id_result = parse_qs(urlparse(a_tag.attrs["href"]).query).get(
                        "ID", None
                    )
                    if id_result:
                        meeting_id = id_result[0]
                        break

                meeting = ResolutionMeeting(
                    meetingId=int(meeting_id) if meeting_id else None,
                    timestamp=meeting_datetime,
                )
                resolution.meetings.append(meeting)

                # temp storage before we can init a table entry
                voting_entry = {"custom_vote_types": {}}

                for row in voting_record.find_all("tr"):
                    cols = row.find_all("td")
                    field_name = cols[0].text.replace(":", "").lower()
                    field_value = cols[1].text
                    # history_item["voting"][field_name] = field_value

                    if field_name == "result":
                        voting_entry["result"] = field_value

                    elif field_name == "mover":
                        # for the 'mover' field, split the field into
                        # name and title values
                        split_commas = field_value.split(", ")
                        voting_entry["mover"] = split_commas.pop(0)

                        # mover_title = None
                        # if split_commas:
                        #    mover_title = split_commas[0]
                    elif field_name == "seconder":
                        pass

                    else:
                        voting_entry["custom_vote_types"][field_name] = (
                            field_value.split(", ")
                        )

                must_refresh_people = False
                must_refresh_vote_types = False

                # resolve all voters to People
                # voters -> People: mover
                if voting_entry["mover"] not in self.people_name_to_id:
                    self.add_person(voting_entry["mover"])
                    must_refresh_people = True
                    if must_refresh_people:
                        self.refresh_people()

                # voters -> People: custom vote_types
                for voters in voting_entry["custom_vote_types"].values():
                    for voter in voters:
                        if voter not in self.people_name_to_id:
                            self.add_person(voter)
                            must_refresh_people = True

                # resolve custom vote types
                for vote_type in voting_entry["custom_vote_types"].keys():
                    if vote_type not in self.custom_vote_type_name_to_id:
                        self.add_custom_vote_type(vote_type)
                        must_refresh_vote_types = True

                # only refresh if we need to
                if must_refresh_people:
                    self.refresh_people()
                if must_refresh_vote_types:
                    self.refresh_custom_vote_types()

                # map voters to their vote types
                person_votes = list()
                for vote_type, voters in voting_entry["custom_vote_types"].items():
                    for voter in voters:
                        person_votes.append(
                            PersonVote(
                                person_id=self.people_name_to_id[voter],
                                vote_type_id=self.custom_vote_type_name_to_id[
                                    vote_type
                                ],
                            )
                        )

                new_vote = ResolutionVote(
                    result=voting_entry["result"],
                    mover_id=self.people_name_to_id[voting_entry["mover"]],
                    resolution_id=resolution_id,
                    person_votes=person_votes,
                )

                resolution.votes.append(new_vote)

        return resolution


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Path to config file to use, defaults to ./config.json",
        default="config.json",
    )

    args = parser.parse_args()

    if not os.path.exists(args.config):
        print("invalid path to config")
        return

    with open(args.config, "r") as f:
        config = json.load(f)

    # connect to / init DB
    engine = create_engine(config["database_engine_uri"], echo=True)
    Base.metadata.create_all(engine)
    database_session = sessionmaker(bind=engine)()

    # init scraper
    iqm_scraper = IqmScraper(config["iqm_root_url"], database_session)

    recorded_resolution_ids = [
        res_id for (res_id,) in database_session.query(Resolution.id).all()
    ]
    resolution_id = -1

    try:
        for resolution_id in range(*config["resolution_range"]):
            # skip ones we've already seen
            if resolution_id in recorded_resolution_ids:
                continue

            if resolution_id % 10 == 0:
                print(resolution_id)

                # commit every ~1000 rows
                if resolution_id % 1000 == 0:
                    database_session.commit()

            resolution_info = iqm_scraper.get_resolution(
                resolution_id, include_body=config.get("include_body", True)
            )
            if resolution_info:
                database_session.add(resolution_info)

    except BaseException as be:
        print(f"{resolution_id} - {be}")

    finally:
        database_session.commit()


if __name__ == "__main__":
    main()
